from __future__ import annotations

import errno
import gc
import socket
import threading
import time
import weakref

import pytest

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx.exceptions import ConnectionClosedError, ResponseTimeoutError, TransportCloseTimeoutError
from eltdx.protocol.constants import TYPE_SECURITY_COUNT
from eltdx.transport import socket as socket_module
from eltdx.transport.actor import cancel_ticket
from eltdx.transport import actor as actor_module
from eltdx.transport import pool as pool_module
from eltdx.transport.pool import LeaseBroker, PinCompletion, PinWaiter, PinnedTransportProxy, PooledSocketTransport
from eltdx.transport.push import PushBuffer


class DelayedSetEvent:
    def __init__(
        self,
        *,
        first_waiting: threading.Event,
        delayed_set_entered: threading.Event,
        allow_delayed_set: threading.Event,
        reused_waiting: threading.Event,
    ) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._wait_calls = 0
        self._set_calls = 0
        self._first_waiting = first_waiting
        self._delayed_set_entered = delayed_set_entered
        self._allow_delayed_set = allow_delayed_set
        self._reused_waiting = reused_waiting

    def wait(self, timeout: float | None = None) -> bool:
        with self._lock:
            self._wait_calls += 1
            call = self._wait_calls
        if call == 1:
            self._first_waiting.set()
            assert self._delayed_set_entered.wait(timeout=2)
            return False
        self._reused_waiting.set()
        return self._event.wait(timeout)

    def set(self) -> None:
        with self._lock:
            self._set_calls += 1
            call = self._set_calls
        if call == 1:
            self._delayed_set_entered.set()
            assert self._allow_delayed_set.wait(timeout=2)
        self._event.set()

    def clear(self) -> None:
        self._event.clear()

    def is_set(self) -> bool:
        return self._event.is_set()


class BlockingSetEvent:
    def __init__(self, set_entered: threading.Event, allow_set: threading.Event) -> None:
        self._event = threading.Event()
        self._set_entered = set_entered
        self._allow_set = allow_set

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)

    def set(self) -> None:
        self._set_entered.set()
        assert self._allow_set.wait(timeout=2)
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()


class DelayedReturnEvent:
    def __init__(self, return_entered: threading.Event, allow_return: threading.Event) -> None:
        self._event = threading.Event()
        self._return_entered = return_entered
        self._allow_return = allow_return

    def wait(self, timeout: float | None = None) -> bool:
        result = self._event.wait(timeout)
        if result:
            self._return_entered.set()
            assert self._allow_return.wait(timeout=2)
        return result

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()


def test_admission_waiter_late_set_cannot_wake_next_acquire(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=4)
    initial = broker.acquire(time.monotonic() + 2)
    first_waiting = threading.Event()
    delayed_set_entered = threading.Event()
    allow_delayed_set = threading.Event()
    reused_waiting = threading.Event()
    first_acquired = threading.Event()
    second_done = threading.Event()
    real_second = threading.Event()
    delayed = DelayedSetEvent(
        first_waiting=first_waiting,
        delayed_set_entered=delayed_set_entered,
        allow_delayed_set=allow_delayed_set,
        reused_waiting=reused_waiting,
    )
    events = iter((delayed, real_second))
    first_lease = []
    result: list[object] = []

    def worker() -> None:
        lease = broker.acquire(time.monotonic() + 2)
        first_lease.append(lease)
        first_acquired.set()
        try:
            second = broker.acquire(time.monotonic() + 2)
        except BaseException as exc:
            result.append(exc)
        else:
            result.append("second acquired")
            broker.release(second)
        finally:
            second_done.set()

    worker_thread = threading.Thread(target=worker)
    release_thread = threading.Thread(target=lambda: broker.release(initial))
    monkeypatch.setattr(pool_module.threading, "Event", lambda: next(events))

    worker_thread.start()
    assert first_waiting.wait(timeout=2)
    assert broker.wait_for_waiters(1)
    release_thread.start()
    assert delayed_set_entered.wait(timeout=2)
    assert first_acquired.wait(timeout=2)
    assert broker.wait_for_waiters(1)
    allow_delayed_set.set()
    woke_from_old_set = second_done.wait(timeout=0.05)
    assert broker.release(first_lease[0])
    assert second_done.wait(timeout=2)
    worker_thread.join(timeout=2)
    release_thread.join(timeout=2)
    snapshot = broker.snapshot()
    broker.close()

    assert not worker_thread.is_alive() and not release_thread.is_alive()
    assert not woke_from_old_set
    assert result == ["second acquired"]
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.active_leases) == (1, 0, 0)


def test_broker_release_cannot_assign_waiter_after_deadline() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    initial = broker.acquire(time.monotonic() + 1)
    deadline = time.monotonic() + 0.05
    results: list[object] = []

    def acquire() -> None:
        try:
            results.append(broker.acquire(deadline))
        except BaseException as exc:
            results.append(exc)

    waiter = threading.Thread(target=acquire)

    waiter.start()
    assert broker.wait_for_waiters(1)
    with broker._condition:
        broker._waiters[0].deadline = time.monotonic() - 1
        assert broker.release(initial)
    waiter.join(timeout=2)

    assert not waiter.is_alive()
    assert len(results) == 1 and isinstance(results[0], ResponseTimeoutError)
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.active_leases) == (1, 0, 0)


def test_broker_close_before_assignment_wakeup_rejects_released_lease(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    initial = broker.acquire(time.monotonic() + 1)
    set_entered = threading.Event()
    allow_set = threading.Event()
    completed = BlockingSetEvent(set_entered, allow_set)
    results: list[object] = []

    def acquire() -> None:
        try:
            results.append(broker.acquire(time.monotonic() + 1))
        except BaseException as exc:
            results.append(exc)

    waiter = threading.Thread(target=acquire)
    releaser = threading.Thread(target=lambda: broker.release(initial))
    monkeypatch.setattr(pool_module.threading, "Event", lambda: completed)
    waiter.start()
    assert broker.wait_for_waiters(1)
    releaser.start()
    assert set_entered.wait(timeout=2)
    broker.close()
    allow_set.set()
    waiter.join(timeout=2)
    releaser.join(timeout=2)

    assert not waiter.is_alive() and not releaser.is_alive()
    assert len(results) == 1 and isinstance(results[0], ConnectionClosedError)
    snapshot = broker.snapshot()
    assert snapshot.closed and snapshot.active_leases == 0 and snapshot.idle_slots == 0


def test_pool_connect_holds_broker_slot_against_concurrent_execute(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    broker, _ = pool._ensure_started()
    slot = pool._transports[0]
    connect_entered = threading.Event()
    execute_entered = threading.Event()
    allow_connect = threading.Event()
    connect_results: list[object] = []
    execute_results: list[object] = []

    def connect(**_kwargs) -> None:
        connect_entered.set()
        assert allow_connect.wait(timeout=2)

    def execute(*_args, completion, **_kwargs) -> int:
        execute_entered.set()
        completion(None)
        return 73

    def run_connect() -> None:
        try:
            connect_results.append(pool.connect())
        except BaseException as exc:
            connect_results.append(exc)

    def run_execute() -> None:
        try:
            execute_results.append(pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}))
        except BaseException as exc:
            execute_results.append(exc)

    monkeypatch.setattr(slot, "_connect_with_deadline", connect)
    monkeypatch.setattr(slot, "_execute_with_lease", execute)
    connector = threading.Thread(target=run_connect)
    executor = threading.Thread(target=run_execute)
    connector.start()
    try:
        assert connect_entered.wait(timeout=2)
        executor.start()
        assert broker.wait_for_waiters(1)
        assert not execute_entered.is_set()
    finally:
        allow_connect.set()
    connector.join(timeout=2)
    executor.join(timeout=2)

    assert not connector.is_alive() and not executor.is_alive()
    assert connect_results == [None]
    assert execute_results == [73]
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.active_leases) == (1, 0, 0)
    pool.close()


def test_old_pool_connect_cannot_block_or_clear_reopened_epoch(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    old_broker, _ = pool._ensure_started()
    old_epoch = old_broker.pool_epoch
    slot = pool._transports[0]
    old_entered = threading.Event()
    new_entered = threading.Event()
    allow_old = threading.Event()
    allow_new = threading.Event()
    old_results: list[object] = []
    new_results: list[object] = []

    def connect(*, expected_runtime_epoch, **_kwargs) -> None:
        if expected_runtime_epoch == old_epoch:
            old_entered.set()
            assert allow_old.wait(timeout=2)
            raise ConnectionClosedError("old connect failed after close")
        new_entered.set()
        assert allow_new.wait(timeout=2)

    def run(results: list[object]) -> None:
        try:
            results.append(pool.connect())
        except BaseException as exc:
            results.append(exc)

    monkeypatch.setattr(slot, "_connect_with_deadline", connect)
    old_connector = threading.Thread(target=lambda: run(old_results))
    new_connector = threading.Thread(target=lambda: run(new_results))
    old_connector.start()
    assert old_entered.wait(timeout=2)
    pool.close()
    new_connector.start()
    try:
        assert new_entered.wait(timeout=2)
        with pool._condition:
            new_broker = pool._broker
            assert new_broker is not None and new_broker is not old_broker
            assert pool._connect_broker is new_broker
        allow_old.set()
        old_connector.join(timeout=2)
        assert not old_connector.is_alive()
        with pool._condition:
            assert pool._connect_broker is new_broker
    finally:
        allow_old.set()
        allow_new.set()
    new_connector.join(timeout=2)

    assert not new_connector.is_alive()
    assert len(old_results) == 1 and isinstance(old_results[0], ConnectionClosedError)
    assert new_results == [None]
    with pool._condition:
        assert pool._connect_broker is None
        assert pool._broker is new_broker
        assert pool._state is pool_module.PoolState.RUNNING
    pool.close()


class FakePinnedSlot:
    connected_host = "127.0.0.1:7709"
    last_handshake = {"server": "test"}
    last_heartbeat = {"ok": True}
    _runtime = None

    def __init__(self) -> None:
        self.cancelled: list[int] = []

    def _cancel_lease(self, lease_id: int) -> None:
        self.cancelled.append(lease_id)


def _new_fake_proxy(timeout: float = 0.05):
    broker = LeaseBroker(2, pool_size=1, max_pending_requests=4)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)
    slot = FakePinnedSlot()
    proxy = PinnedTransportProxy(broker, lease, slot, PushBuffer(2), timeout)
    return broker, lease, slot, proxy


def test_pinned_close_timeout_can_finish_cleanup_and_restore_capacity() -> None:
    broker, lease, slot, proxy = _new_fake_proxy(timeout=0.02)
    proxy._active_call = 1

    with pytest.raises(TransportCloseTimeoutError, match="did not quiesce"):
        proxy.close()
    proxy._wire_terminal(1)
    proxy.close()
    proxy.close()

    snapshot = broker.snapshot()
    assert slot.cancelled == [lease.lease_id]
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)


def test_failed_pin_completion_retains_cleanup_owner_until_terminal() -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=0.01)
    proxy._active_call = 1
    completion = PinCompletion(proxy, 1)

    with pytest.raises(TransportCloseTimeoutError, match="did not quiesce"):
        proxy.close()
    reference = weakref.ref(proxy)
    del proxy
    gc.collect()

    assert reference() is not None
    assert completion._proxy is reference()
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (0, 1)

    completion(None)
    gc.collect()

    assert completion._proxy is None
    assert reference() is None
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)


def test_pinned_close_releases_each_waiter_reservation_exactly_once(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1)
    broker.reserve_pin_waiter()
    proxy._active_call = 1
    release_entered = threading.Event()
    allow_release = threading.Event()
    waiter_done = threading.Event()
    close_done = threading.Event()
    result: list[BaseException] = []
    original_release = broker.release_pin_waiter

    def delayed_release(completed=None) -> None:
        original_release(completed)
        release_entered.set()
        assert allow_release.wait(timeout=2)

    monkeypatch.setattr(broker, "release_pin_waiter", delayed_release)

    def wait_for_pin() -> None:
        try:
            proxy._admit(time.monotonic() + 0.1)
        except BaseException as exc:
            result.append(exc)
        finally:
            waiter_done.set()

    waiter = threading.Thread(target=wait_for_pin)
    waiter.start()
    assert broker.wait_for_pin_waiters(2)
    with proxy._condition:
        proxy._active_call = None
    closer = threading.Thread(target=lambda: (proxy.close(), close_done.set()))
    closer.start()
    assert release_entered.wait(timeout=2)
    assert broker.snapshot().pin_waiter_count == 1
    assert not waiter_done.is_set()
    allow_release.set()
    assert waiter_done.wait(timeout=2)
    waiter.join(timeout=2)
    closer.join(timeout=2)

    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
    assert broker.snapshot().pin_waiter_count == 1
    assert close_done.is_set()
    original_release()
    assert broker.snapshot().pin_waiter_count == 0


def test_pinned_waiter_reservation_cannot_invert_local_fifo(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=2)
    proxy._active_call = 1
    proxy._call_counter = 1
    first_reserve_entered = threading.Event()
    allow_first_reserve = threading.Event()
    second_reserve_entered = threading.Event()
    original_reserve = broker.reserve_pin_waiter
    reserve_calls = 0
    reserve_lock = threading.Lock()
    results: list[BaseException] = []

    def controlled_reserve(completed=None) -> None:
        nonlocal reserve_calls
        with reserve_lock:
            reserve_calls += 1
            call = reserve_calls
        if call == 1:
            first_reserve_entered.set()
            assert allow_first_reserve.wait(timeout=2)
        else:
            second_reserve_entered.set()
        original_reserve(completed)

    monkeypatch.setattr(broker, "reserve_pin_waiter", controlled_reserve)

    def admit() -> None:
        try:
            proxy._admit(time.monotonic() + 2)
        except BaseException as exc:
            results.append(exc)

    first = threading.Thread(target=admit)
    second = threading.Thread(target=admit)
    first.start()
    assert first_reserve_entered.wait(timeout=2)
    condition_was_free = proxy._condition.acquire(blocking=False)
    if condition_was_free:
        proxy._condition.release()
    second.start()
    allow_first_reserve.set()
    assert second_reserve_entered.wait(timeout=2)
    assert broker.wait_for_pin_waiters(2)
    with proxy._condition:
        queued_call_ids = [waiter.call_id for waiter in proxy._waiters]
        proxy._active_call = None
    proxy.close()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not condition_was_free
    assert queued_call_ids == [2, 3]
    assert all(isinstance(item, ConnectionClosedError) for item in results)


def test_pin_terminal_skips_expired_waiter_before_assigning_live_waiter() -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1)
    expired = PinWaiter(2, time.monotonic() - 1, reserved=True)
    live = PinWaiter(3, time.monotonic() + 1, reserved=True)
    broker.reserve_pin_waiter(expired.completed)
    broker.reserve_pin_waiter(live.completed)
    proxy._active_call = 1
    proxy._call_counter = 3
    proxy._waiters.extend((expired, live))

    proxy._wire_terminal(1)

    assert isinstance(expired.error, ResponseTimeoutError)
    assert expired.completed.is_set() and not expired.assigned and not expired.reserved
    assert live.completed.is_set() and live.assigned and not live.reserved
    assert proxy._active_call == live.call_id
    assert broker.snapshot().pin_waiter_count == 0
    proxy._wire_terminal(live.call_id)
    proxy.close()


def test_pin_close_before_assignment_wakeup_rejects_unstarted_call(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1)
    proxy._active_call = 1
    proxy._call_counter = 1
    set_entered = threading.Event()
    allow_set = threading.Event()
    completed = BlockingSetEvent(set_entered, allow_set)
    admit_results: list[object] = []
    close_results: list[object] = []

    def admit() -> None:
        try:
            admit_results.append(proxy._admit(time.monotonic() + 1))
        except BaseException as exc:
            admit_results.append(exc)

    def close() -> None:
        try:
            close_results.append(proxy.close())
        except BaseException as exc:
            close_results.append(exc)

    waiter = threading.Thread(target=admit)
    terminal = threading.Thread(target=lambda: proxy._wire_terminal(1))
    closer = threading.Thread(target=close)
    monkeypatch.setattr(
        pool_module,
        "PinWaiter",
        lambda call_id, deadline, **kwargs: PinWaiter(call_id, deadline, completed=completed, **kwargs),
    )
    waiter.start()
    assert broker.wait_for_pin_waiters(1)
    terminal.start()
    assert set_entered.wait(timeout=2)
    closer.start()
    with proxy._condition:
        assert proxy._condition.wait_for(lambda: proxy._state is pool_module.PinState.CLOSING, timeout=2)
    allow_set.set()
    waiter.join(timeout=2)
    terminal.join(timeout=2)
    closer.join(timeout=2)

    assert not waiter.is_alive() and not terminal.is_alive() and not closer.is_alive()
    assert len(admit_results) == 1 and isinstance(admit_results[0], ConnectionClosedError)
    assert close_results == [None]
    assert proxy._active_call is None
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.pin_waiter_count, snapshot.active_leases) == (1, 0, 0)


def test_pool_close_wakes_all_pin_local_waiters_after_terminal() -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=5)
    proxy._active_call = 1
    proxy._call_counter = 1
    results: list[object] = []

    def admit() -> None:
        try:
            results.append(proxy._admit(time.monotonic() + 5))
        except BaseException as exc:
            results.append(exc)

    waiters = [threading.Thread(target=admit) for _ in range(2)]
    for waiter in waiters:
        waiter.start()
    assert broker.wait_for_pin_waiters(2)

    broker.close()
    proxy._wire_terminal(1)
    for waiter in waiters:
        waiter.join(timeout=0.5)
    alive_after_terminal = [waiter for waiter in waiters if waiter.is_alive()]
    with proxy._condition:
        remaining_waiters = len(proxy._waiters)
        active_call = proxy._active_call
        state = proxy._state

    if alive_after_terminal:
        proxy.close()
        for waiter in waiters:
            waiter.join(timeout=2)

    assert not alive_after_terminal
    assert len(results) == 2
    assert all(isinstance(item, ConnectionClosedError) for item in results)
    assert remaining_waiters == 0
    assert active_call is None
    assert state is pool_module.PinState.CLOSED
    snapshot = broker.snapshot()
    assert snapshot.closed
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.pin_waiter_count, snapshot.active_leases) == (0, 0, 0, 0)


def test_broker_close_wakes_queued_pin_waiter_while_assigned_consumer_is_delayed(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=5)
    proxy._active_call = 1
    proxy._call_counter = 1
    first_return_entered = threading.Event()
    allow_first_return = threading.Event()
    first_completed = DelayedReturnEvent(first_return_entered, allow_first_return)
    second_completed = threading.Event()
    completed_events = iter((first_completed, second_completed))
    results: dict[str, object] = {}
    second_done = threading.Event()

    monkeypatch.setattr(
        pool_module,
        "PinWaiter",
        lambda call_id, deadline, **kwargs: PinWaiter(
            call_id,
            deadline,
            completed=next(completed_events),
            **kwargs,
        ),
    )

    def admit(name: str, done: threading.Event | None = None) -> None:
        try:
            results[name] = proxy._admit(time.monotonic() + 5)
        except BaseException as exc:
            results[name] = exc
        finally:
            if done is not None:
                done.set()

    first = threading.Thread(target=admit, args=("first",))
    second = threading.Thread(target=admit, args=("second", second_done))
    first.start()
    assert broker.wait_for_pin_waiters(1)
    second.start()
    assert broker.wait_for_pin_waiters(2)

    proxy._wire_terminal(1)
    assert first_return_entered.wait(timeout=2)
    broker.close()
    second_exited_on_broker_close = second_done.wait(timeout=0.5)

    allow_first_return.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert second_exited_on_broker_close
    assert not first.is_alive() and not second.is_alive()
    assert isinstance(results["first"], ConnectionClosedError)
    assert isinstance(results["second"], ConnectionClosedError)
    with proxy._condition:
        assert proxy._active_call is None
        assert not proxy._waiters
        assert proxy._state is pool_module.PinState.CLOSED


def test_failed_pin_releases_lease_when_assigned_consumer_never_started_wire(monkeypatch) -> None:
    broker, lease, _, proxy = _new_fake_proxy(timeout=0.02)
    proxy._active_call = 1
    proxy._call_counter = 1
    return_entered = threading.Event()
    allow_return = threading.Event()
    completed = DelayedReturnEvent(return_entered, allow_return)
    result: list[object] = []

    monkeypatch.setattr(
        pool_module,
        "PinWaiter",
        lambda call_id, deadline, **kwargs: PinWaiter(call_id, deadline, completed=completed, **kwargs),
    )

    def admit() -> None:
        try:
            result.append(proxy._admit(time.monotonic() + 2))
        except BaseException as exc:
            result.append(exc)

    waiter = threading.Thread(target=admit)
    waiter.start()
    assert broker.wait_for_pin_waiters(1)
    proxy._wire_terminal(1)
    assert return_entered.wait(timeout=2)

    with pytest.raises(TransportCloseTimeoutError, match="did not quiesce"):
        proxy.close()
    assert proxy._state is pool_module.PinState.FAILED
    assert proxy._active_call == 2
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (0, 1)

    allow_return.set()
    waiter.join(timeout=2)
    snapshot = broker.snapshot()
    leaked = (snapshot.idle_slots, snapshot.active_leases) != (1, 0)
    if leaked:
        proxy.close()

    assert not waiter.is_alive()
    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
    assert proxy._active_call is None
    assert proxy._state is pool_module.PinState.CLOSED
    assert not leaked
    assert lease.state is pool_module.LeaseState.RELEASED


class BlockingConnectSlot(FakePinnedSlot):
    def __init__(self, entered: threading.Event, release: threading.Event) -> None:
        super().__init__()
        self._entered = entered
        self._release = release

    def connect(self) -> None:
        self._entered.set()
        assert self._release.wait(timeout=2)

    def _connect_with_deadline(
        self,
        *,
        deadline,
        completion,
        runtime=None,
        lock_slot=True,
        lease_id=0,
        expected_runtime_epoch=None,
    ) -> None:
        try:
            self.connect()
        finally:
            completion(None)


def test_pinned_connect_is_an_active_operation_for_close() -> None:
    entered = threading.Event()
    release = threading.Event()
    connect_done = threading.Event()
    close_done = threading.Event()
    broker = LeaseBroker(3, pool_size=1, max_pending_requests=4)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)
    slot = BlockingConnectSlot(entered, release)
    proxy = PinnedTransportProxy(broker, lease, slot, PushBuffer(3), timeout=1)

    connect_thread = threading.Thread(target=lambda: (proxy.connect(), connect_done.set()))
    close_thread = threading.Thread(target=lambda: (proxy.close(), close_done.set()))
    connect_thread.start()
    assert entered.wait(timeout=2)
    close_thread.start()
    closed_while_connecting = close_done.wait(timeout=0.05)
    snapshot_while_connecting = broker.snapshot()
    release.set()
    connect_thread.join(timeout=2)
    close_thread.join(timeout=2)

    assert not closed_while_connecting
    assert (snapshot_while_connecting.idle_slots, snapshot_while_connecting.active_leases) == (0, 1)
    assert connect_done.is_set() and close_done.is_set()
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)


def test_pinned_proxy_preserves_connection_snapshot_properties() -> None:
    broker, _, slot, proxy = _new_fake_proxy()
    try:
        assert proxy.connected_host == slot.connected_host
        assert proxy.last_handshake == slot.last_handshake
        assert proxy.last_heartbeat == slot.last_heartbeat
    finally:
        proxy.close()
        broker.close()


def test_late_cancel_on_reused_pinned_lease_is_noop(monkeypatch) -> None:
    b_received = threading.Event()
    respond_to_b = threading.Event()
    release_server = threading.Event()
    captured_tickets = []
    original_submit = socket_module.submit_request

    def capture_submit(*args, **kwargs):
        ticket = original_submit(*args, **kwargs)
        captured_tickets.append(ticket)
        return ticket

    monkeypatch.setattr(socket_module, "submit_request", capture_submit)

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (111).to_bytes(2, "little")))
        msg_id, msg_type, _ = read_request(conn)
        b_received.set()
        assert respond_to_b.wait(timeout=2)
        conn.sendall(response_bytes(msg_id, msg_type, (222).to_bytes(2, "little")))
        assert release_server.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport([server.host], pool_size=1, timeout=1, heartbeat_interval=None)
        try:
            with pool.pin() as pinned:
                assert pinned.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 111
                a_ticket = captured_tickets[0]
                result: list[object] = []

                def run_b() -> None:
                    try:
                        result.append(pinned.execute(TYPE_SECURITY_COUNT, {"market": "sz"}))
                    except BaseException as exc:
                        result.append(exc)

                thread = threading.Thread(target=run_b)
                thread.start()
                assert b_received.wait(timeout=2)
                runtime = pinned._slot._runtime
                assert runtime is not None
                cancel_ticket(runtime, a_ticket)
                respond_to_b.set()
                thread.join(timeout=2)
                assert not thread.is_alive()
                assert result == [222]
                assert captured_tickets[1].request_id != a_ticket.request_id
        finally:
            respond_to_b.set()
            release_server.set()
            pool.close()


class StalledConnectSocket:
    def __init__(self, family: int, socktype: int, proto: int) -> None:
        self._socket, self._peer = socket.socketpair()

    def setblocking(self, value: bool) -> None:
        self._socket.setblocking(value)

    def connect_ex(self, address) -> int:
        return errno.EINPROGRESS

    def getsockopt(self, level: int, option: int) -> int:
        return errno.EINPROGRESS

    def fileno(self) -> int:
        return self._socket.fileno()

    def close(self) -> None:
        self._socket.close()
        self._peer.close()


def test_pinned_close_cancels_exact_connect_ticket_and_restores_capacity(monkeypatch) -> None:
    runtimes = []
    tickets = []
    submitted = threading.Event()
    original_start = socket_module.start_actor
    original_submit = socket_module.submit_connect

    def start_stalled(epoch, endpoints, **kwargs):
        runtime = original_start(epoch, endpoints, socket_factory=StalledConnectSocket, **kwargs)
        runtimes.append(runtime)
        return runtime

    def capture_submit(*args, **kwargs):
        ticket = original_submit(*args, **kwargs)
        tickets.append(ticket)
        submitted.set()
        return ticket

    monkeypatch.setattr(socket_module, "start_actor", start_stalled)
    monkeypatch.setattr(socket_module, "submit_connect", capture_submit)
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=2, heartbeat_interval=None)
    context = pool.pin()
    pinned = context.__enter__()
    broker = pool._broker
    assert broker is not None
    result: list[BaseException] = []

    def connect() -> None:
        try:
            pinned.connect()
        except BaseException as exc:
            result.append(exc)

    thread = threading.Thread(target=connect)
    try:
        thread.start()
        assert submitted.wait(timeout=2)
        runtime = runtimes[0]
        ticket = tickets[0]
        assert runtime.generation_started.wait(timeout=2)
        with runtime.control_lock:
            assert runtime.active_task is ticket
        assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (0, 1)

        pinned.close()
        thread.join(timeout=2)

        assert not thread.is_alive()
        assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
        assert ticket.state is actor_module.RequestState.CANCELLED
        assert ticket.completed.is_set()
        with runtime.control_lock:
            assert runtime.active_task is None
            assert runtime.pending_task is None
        assert runtime.generation is None
        assert runtime.state is actor_module.RuntimeState.RUNNING
        assert not runtime.stop_requested
        assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)
        replacement = broker.acquire(time.monotonic() + 1)
        assert replacement.slot_id == pinned._lease.slot_id
        assert broker.release(replacement)
        pinned.close()
    finally:
        context.__exit__(None, None, None)
        pool.close()
        thread.join(timeout=2)


def test_pinned_cancel_error_notifies_concurrent_retry_close(monkeypatch) -> None:
    broker, lease, slot, proxy = _new_fake_proxy(timeout=1)
    proxy._active_call = 1
    proxy._call_counter = 1
    first_cancel_entered = threading.Event()
    allow_first_error = threading.Event()
    second_waiting = threading.Event()
    retry_cancel_entered = threading.Event()
    first_done = threading.Event()
    second_done = threading.Event()
    cancel_calls: list[int] = []
    release_calls = []
    errors: list[BaseException] = []
    original_wait = proxy._condition.wait
    original_release = broker.release

    def observed_wait(timeout=None):
        if threading.current_thread().name == "pin-close-retry":
            second_waiting.set()
        return original_wait(timeout)

    def cancel(lease_id: int) -> None:
        cancel_calls.append(lease_id)
        if len(cancel_calls) == 1:
            first_cancel_entered.set()
            assert allow_first_error.wait(timeout=2)
            raise OSError("wakeup failed")
        retry_cancel_entered.set()

    def release(item):
        release_calls.append(item)
        return original_release(item)

    monkeypatch.setattr(proxy._condition, "wait", observed_wait)
    monkeypatch.setattr(slot, "_cancel_lease", cancel)
    monkeypatch.setattr(broker, "release", release)

    def close(done: threading.Event) -> None:
        try:
            proxy.close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()

    first = threading.Thread(target=close, args=(first_done,), name="pin-close-owner")
    second = threading.Thread(target=close, args=(second_done,), name="pin-close-retry")
    first.start()
    assert first_cancel_entered.wait(timeout=2)
    second.start()
    assert second_waiting.wait(timeout=2)
    allow_first_error.set()
    assert first_done.wait(timeout=2)
    assert retry_cancel_entered.wait(timeout=2)
    proxy._wire_terminal(1)
    assert second_done.wait(timeout=2)
    first.join(timeout=2)
    second.join(timeout=2)

    assert len(errors) == 1 and isinstance(errors[0], OSError)
    assert cancel_calls == [lease.lease_id, lease.lease_id]
    assert release_calls == [lease]
    assert proxy._state is pool_module.PinState.CLOSED
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)
    proxy.close()
    assert cancel_calls == [lease.lease_id, lease.lease_id]
    assert release_calls == [lease]


@pytest.mark.parametrize("ticket_kind", ("connect", "request"))
def test_cancel_lease_selects_exact_pending_ticket(ticket_kind: str) -> None:
    lease_id = 73
    runtime = actor_module.ActorRuntime(2, ())
    runtime.state = actor_module.RuntimeState.RUNNING
    if ticket_kind == "connect":
        ticket = actor_module.ConnectTicket(
            runtime_epoch=2,
            deadline=time.monotonic() + 1,
            lease_id=lease_id,
            request_id=11,
        )
    else:
        ticket = actor_module.RequestTicket(
            runtime_epoch=2,
            lease_id=lease_id,
            command=TYPE_SECURITY_COUNT,
            request_payload_snapshot={"market": "sz"},
            deadline=time.monotonic() + 1,
            retry_safe=True,
            request_id=11,
        )
    runtime.pending_task = ticket
    slot = socket_module.SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    slot._runtime = runtime

    slot._cancel_lease(lease_id + 1)
    assert runtime.cancel_requests == {}
    slot._cancel_lease(lease_id)

    token = runtime.cancel_requests[ticket.request_id]
    assert token.runtime_epoch == runtime.runtime_epoch
    assert token.request_id == ticket.request_id
    assert token.lease_id == lease_id
    assert runtime.pending_task is ticket


def test_invalid_payload_releases_normal_and_pinned_capacity() -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=0.1, heartbeat_interval=None)
    broker, _ = pool._ensure_started()

    with pytest.raises(TypeError):
        pool.execute(TYPE_SECURITY_COUNT, 1)  # type: ignore[arg-type]
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)

    with pool.pin() as pinned:
        with pytest.raises(TypeError):
            pinned.execute(TYPE_SECURITY_COUNT, 1)  # type: ignore[arg-type]
        assert pinned._active_call is None
        assert broker.snapshot().active_leases == 1

    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)
    pool.close()


class DeferredTerminalSlot(FakePinnedSlot):
    def __init__(self) -> None:
        super().__init__()
        self.completion = None
        self.cancel_entered = threading.Event()

    def _execute_with_lease(self, command, payload, **kwargs):
        self.completion = kwargs["completion"]
        raise ResponseTimeoutError("deferred Actor terminal")

    def _cancel_lease(self, lease_id: int) -> None:
        super()._cancel_lease(lease_id)
        self.cancel_entered.set()


def test_pinned_timeout_keeps_active_call_until_actor_terminal() -> None:
    broker = LeaseBroker(2, pool_size=1, max_pending_requests=4)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)
    slot = DeferredTerminalSlot()
    proxy = PinnedTransportProxy(broker, lease, slot, PushBuffer(2), timeout=1)

    with pytest.raises(ResponseTimeoutError, match="deferred"):
        proxy.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
    assert proxy._active_call == 1
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (0, 1)

    close_done = threading.Event()
    close_errors: list[BaseException] = []

    def close() -> None:
        try:
            proxy.close()
        except BaseException as exc:
            close_errors.append(exc)
        finally:
            close_done.set()

    closer = threading.Thread(target=close)
    closer.start()
    assert slot.cancel_entered.wait(timeout=2)
    assert not close_done.is_set()
    assert slot.completion is not None
    slot.completion(None)
    closer.join(timeout=2)

    assert not closer.is_alive()
    assert close_errors == []
    assert proxy._state is pool_module.PinState.CLOSED
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)


class BrokenWakeWriter:
    def send(self, data: bytes) -> int:
        raise OSError("wakeup injection")


@pytest.mark.parametrize("ticket_kind", ("connect", "request"))
def test_wakeup_failure_terminalizes_ticket_before_lease_release(ticket_kind: str) -> None:
    broker = LeaseBroker(2, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    completion = pool_module.LeaseCompletion(broker, lease)
    runtime = actor_module.ActorRuntime(2, ())
    runtime.state = actor_module.RuntimeState.RUNNING
    runtime.wake_writer = BrokenWakeWriter()

    if ticket_kind == "connect":
        ticket = actor_module.submit_connect(
            runtime,
            time.monotonic() + 1,
            lease_id=lease.lease_id,
            completion=completion,
        )
    else:
        ticket = actor_module.submit_request(
            runtime,
            lease_id=lease.lease_id,
            command=TYPE_SECURITY_COUNT,
            payload={"market": "sz"},
            deadline=time.monotonic() + 1,
            retry_safe=True,
            completion=completion,
        )

    assert ticket.completed.is_set()
    assert ticket.state is actor_module.RequestState.FAILED
    assert isinstance(ticket.error, OSError)
    assert runtime.pending_task is None
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)
    replacement = broker.acquire(time.monotonic() + 1)
    assert replacement.slot_id == lease.slot_id
    assert broker.release(replacement)
