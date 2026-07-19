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
from eltdx.hosts import resolve_hosts
from eltdx.protocol.constants import TYPE_SECURITY_COUNT
from eltdx.protocol.frame import ResponseFrame
from eltdx.transport import socket as socket_module
from eltdx.transport.actor import cancel_ticket
from eltdx.transport import actor as actor_module
from eltdx.transport import pool as pool_module
from eltdx.transport.pool import LeaseBroker, PinCompletion, PinWaiter, PinnedTransportProxy, PooledSocketTransport
from eltdx.transport.push import PushBuffer, PushFrame


def test_lease_completion_returns_without_waiting_for_broker_condition() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    completion = pool_module.LeaseCompletion(broker, lease)
    entered = threading.Event()
    release = threading.Event()

    def hold_condition() -> None:
        with broker._condition:
            entered.set()
            release.wait()

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert entered.wait(timeout=2)
    done = threading.Event()
    caller = threading.Thread(target=lambda: (completion.publish(None), done.set()))
    caller.start()
    try:
        assert done.wait(timeout=0.2)
        assert lease.cancellation is not None and lease.cancellation.is_set()
    finally:
        release.set()
        holder.join(timeout=2)
        caller.join(timeout=2)
    assert broker.snapshot().idle_slots == 1
    assert broker.snapshot().active_leases == 0


def test_lease_release_publication_wakes_only_first_live_fifo_waiter() -> None:
    class CountingEvent(threading.Event):
        def __init__(self) -> None:
            super().__init__()
            self.set_calls = 0

        def set(self) -> None:
            self.set_calls += 1
            super().set()

    broker = LeaseBroker(1, pool_size=1, max_pending_requests=128)
    lease = broker.acquire(time.monotonic() + 1)
    deadline = time.monotonic() + 1
    cancelled = pool_module.AdmissionWaiter(1, 1, deadline, False, completed=CountingEvent())
    cancelled.cancelled.set()
    expired = pool_module.AdmissionWaiter(
        1,
        2,
        time.monotonic() - 1,
        False,
        completed=CountingEvent(),
    )
    rejected = pool_module.AdmissionWaiter(1, 3, deadline, False, completed=CountingEvent())
    rejected.state = pool_module.AdmissionState.REJECTED
    live = [
        pool_module.AdmissionWaiter(
            1,
            index + 4,
            deadline,
            False,
            completed=CountingEvent(),
        )
        for index in range(100)
    ]
    broker._waiter_snapshot = (cancelled, expired, rejected, *live)

    broker.publish_lease_release(lease)

    events = [waiter.completed for waiter in (cancelled, expired, rejected, *live)]
    assert [event.set_calls for event in events] == [0, 0, 0, 1, *([0] * 99)]
    assert broker.snapshot().idle_slots == 1
    broker.close()


def test_terminal_lease_publication_is_exactly_once(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    lease_completion = pool_module.LeaseCompletion(broker, lease)
    terminal = socket_module._TerminalCompletion(lease_completion)
    real_publish = broker.publish_lease_release
    published: list[pool_module.SlotLease] = []

    def record_publish(item) -> None:
        published.append(item)
        real_publish(item)

    monkeypatch.setattr(broker, "publish_lease_release", record_publish)

    terminal.publish(None)
    terminal.settle(None)

    assert published == [lease]
    assert broker.snapshot().idle_slots == 1
    broker.close()


def test_lease_release_publication_skips_cancelled_stale_head() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    lease = broker.acquire(time.monotonic() + 1)
    deadline = time.monotonic() + 1
    acquired: list[pool_module.SlotLease] = []
    live_thread = threading.Thread(target=lambda: acquired.append(broker.acquire(deadline)))
    live_thread.start()
    assert broker.wait_for_waiters(1)

    with broker._condition:
        stale = pool_module.AdmissionWaiter(1, 999, deadline, False)
        stale.cancelled.set()
        broker._waiters.appendleft(stale)
        broker._waiter_snapshot = tuple(broker._waiters)
        live = broker._waiters[1]

        broker.publish_lease_release(lease)

        assert not stale.completed.is_set()
        assert live.completed.is_set()

    live_thread.join(timeout=0.2)
    assert not live_thread.is_alive()
    assert len(acquired) == 1
    assert broker.release(acquired[0])
    broker.close()


def test_lease_release_pulse_survives_stale_snapshot_during_assignment(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    first_lease, second_lease = broker.acquire_many(2, time.monotonic() + 1)
    deadline = time.monotonic() + 1
    orphan = pool_module.AdmissionWaiter(1, 999, deadline, False)
    with broker._condition:
        broker._waiters.append(orphan)
        broker._waiter_snapshot = tuple(broker._waiters)

    acquired: list[pool_module.SlotLease] = []
    live_thread = threading.Thread(target=lambda: acquired.append(broker.acquire(deadline)))
    live_thread.start()
    assert broker.wait_for_waiters(2)

    real_new_lease = broker._new_lease_locked
    assignment_paused = threading.Event()
    allow_assignment = threading.Event()
    pause_once = True

    def gated_new_lease(*args, **kwargs):
        nonlocal pause_once
        if pause_once:
            pause_once = False
            assignment_paused.set()
            assert allow_assignment.wait(timeout=2)
        return real_new_lease(*args, **kwargs)

    monkeypatch.setattr(broker, "_new_lease_locked", gated_new_lease)
    release_result: list[bool] = []
    releaser = threading.Thread(target=lambda: release_result.append(broker.release(first_lease)))
    releaser.start()
    assert assignment_paused.wait(timeout=2)

    broker.publish_lease_release(second_lease)
    allow_assignment.set()
    releaser.join(timeout=0.5)
    live_thread.join(timeout=0.5)

    assert not releaser.is_alive() and not live_thread.is_alive()
    assert release_result == [True]
    assert len(acquired) == 1
    assert orphan.assigned_lease is not None
    assert broker.release(orphan.assigned_lease)
    assert broker.release(acquired[0])
    broker.close()


def test_failed_lease_publication_can_retry_before_becoming_idempotent(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    completion = pool_module.LeaseCompletion(broker, lease)
    real_publish = broker.publish_lease_release
    calls = 0

    def fail_once(item) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("publication failed")
        real_publish(item)

    monkeypatch.setattr(broker, "publish_lease_release", fail_once)

    with pytest.raises(RuntimeError, match="publication failed"):
        completion.publish(None)
    completion.publish(None)
    completion.settle(None)

    assert calls == 2
    assert broker.snapshot().idle_slots == 1
    broker.close()


def test_interrupted_head_hands_publication_baton_to_live_successor(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    lease = broker.acquire(time.monotonic() + 1)
    real_waiter = pool_module.AdmissionWaiter
    created = 0
    interrupted = threading.Event()

    class InterruptEvent(threading.Event):
        def wait(self, timeout=None) -> bool:
            super().wait(timeout)
            interrupted.set()
            raise KeyboardInterrupt

    def waiter_factory(*args, **kwargs):
        nonlocal created
        created += 1
        if created == 1:
            kwargs["completed"] = InterruptEvent()
        return real_waiter(*args, **kwargs)

    monkeypatch.setattr(pool_module, "AdmissionWaiter", waiter_factory)
    results: list[tuple[str, object]] = []
    first_done = threading.Event()

    def acquire(label: str, deadline: float) -> None:
        try:
            results.append((label, broker.acquire(deadline)))
        except BaseException as exc:
            results.append((label, exc))
        finally:
            if label == "first":
                first_done.set()

    first = threading.Thread(target=acquire, args=("first", time.monotonic() + 0.15))
    second = threading.Thread(target=acquire, args=("second", time.monotonic() + 1))
    first.start()
    assert broker.wait_for_waiters(1)
    second.start()
    assert broker.wait_for_waiters(2)
    with broker._condition:
        second_waiter = broker._waiters[1]

    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with broker._condition:
            condition_held.set()
            release_condition.wait(timeout=2)

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    try:
        broker.publish_lease_release(lease)
        assert interrupted.wait(timeout=0.2)
        assert first_done.wait(timeout=0.4)
        assert second_waiter.completed.is_set()
    finally:
        release_condition.set()
        holder.join(timeout=2)

    first.join(timeout=0.2)
    second.join(timeout=0.5)
    assert not first.is_alive() and not second.is_alive()
    assert len(results) == 2
    first_result = next(item for label, item in results if label == "first")
    second_result = next(item for label, item in results if label == "second")
    assert isinstance(first_result, KeyboardInterrupt)
    assert isinstance(second_result, pool_module.SlotLease)
    assert broker.release(second_result)
    broker.close()


def test_deferred_lease_completion_wakes_existing_capacity_waiter() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    completion = pool_module.LeaseCompletion(
        broker,
        lease,
        deadline=time.monotonic() + 0.05,
    )
    waiter_deadline = time.monotonic() + 0.6
    acquired: list[object] = []
    waiter = threading.Thread(target=lambda: acquired.append(broker.acquire(waiter_deadline)))
    waiter.start()
    assert broker.wait_for_waiters(1)
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with broker._condition:
            condition_held.set()
            release_condition.wait()

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    try:
        completion(None)
    finally:
        release_condition.set()
        holder.join(timeout=2)
    waiter.join(timeout=0.2)

    assert not waiter.is_alive()
    assert len(acquired) == 1
    assert broker.release(acquired[0])


def test_deferred_lease_publication_preserves_non_head_fifo_waiter() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    lease = broker.acquire(time.monotonic() + 1)
    acquired: list[tuple[str, object]] = []

    def acquire(label: str) -> None:
        try:
            acquired.append((label, broker.acquire(time.monotonic() + 1)))
        except BaseException as exc:
            acquired.append((label, exc))

    first = threading.Thread(target=acquire, args=("first",))
    second = threading.Thread(target=acquire, args=("second",))
    first.start()
    assert broker.wait_for_waiters(1)
    second.start()
    assert broker.wait_for_waiters(2)

    pool_module.LeaseCompletion(broker, lease).publish(None)
    first.join(timeout=0.2)
    assert not first.is_alive()
    assert second.is_alive()
    assert len(acquired) == 1 and acquired[0][0] == "first"
    first_lease = acquired[0][1]
    assert not isinstance(first_lease, BaseException)

    assert broker.release(first_lease)
    second.join(timeout=0.2)
    assert not second.is_alive()
    assert len(acquired) == 2 and acquired[1][0] == "second"
    second_lease = acquired[1][1]
    assert not isinstance(second_lease, BaseException)
    assert broker.release(second_lease)


def test_duplicate_lease_publish_and_settle_reclaims_exactly_once(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    completion = pool_module.LeaseCompletion(broker, lease)
    release_calls = 0
    real_release = broker.release

    def count_release(*args, **kwargs):
        nonlocal release_calls
        release_calls += 1
        return real_release(*args, **kwargs)

    monkeypatch.setattr(broker, "release", count_release)
    completion.publish(None)
    completion.publish(None)
    threads = [threading.Thread(target=completion.settle) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()
    snapshot = broker.snapshot()
    assert release_calls == 1
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)


def test_lease_completion_retains_unexpected_release_error(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    completion = pool_module.LeaseCompletion(broker, lease)
    monkeypatch.setattr(
        broker,
        "release",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("release failed")),
    )

    with pytest.raises(RuntimeError, match="release failed"):
        completion.settle()
    assert lease.cancellation is not None and lease.cancellation.is_set()


def test_wait_ticket_surfaces_internal_lease_settlement_error(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    lease_completion = pool_module.LeaseCompletion(broker, lease)
    completion = socket_module._TerminalCompletion(lease_completion)
    ticket = actor_module.RequestTicket(
        runtime_epoch=1,
        lease_id=lease.lease_id,
        command=TYPE_SECURITY_COUNT,
        request_payload_snapshot={"market": "sz"},
        deadline=time.monotonic() + 1,
        retry_safe=False,
        completion=completion,
    )
    release_error = RuntimeError("lease release failed")
    monkeypatch.setattr(
        broker,
        "release",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(release_error),
    )
    actor_module._complete_ticket(
        ticket,
        actor_module.RequestState.SUCCESS,
        result=object(),
    )

    with pytest.raises(RuntimeError, match="lease release failed") as raised:
        actor_module.wait_ticket(ticket)

    assert raised.value is release_error
    assert ticket.completion_error is release_error


@pytest.mark.parametrize("terminal_kind", ("wire_error", "late_success"))
def test_wait_ticket_chains_internal_settlement_error_to_terminal_error(
    monkeypatch,
    terminal_kind: str,
) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    lease_completion = pool_module.LeaseCompletion(broker, lease)
    completion = socket_module._TerminalCompletion(lease_completion)
    deadline = time.monotonic() - 0.01 if terminal_kind == "late_success" else time.monotonic() + 1
    ticket = actor_module.RequestTicket(
        runtime_epoch=1,
        lease_id=lease.lease_id,
        command=TYPE_SECURITY_COUNT,
        request_payload_snapshot={"market": "sz"},
        deadline=deadline,
        retry_safe=False,
        completion=completion,
    )
    cleanup_error = RuntimeError("lease cleanup failed")
    monkeypatch.setattr(
        broker,
        "release",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(cleanup_error),
    )
    wire_error = ResponseTimeoutError("wire failed")
    actor_module._complete_ticket(
        ticket,
        actor_module.RequestState.SUCCESS if terminal_kind == "late_success" else actor_module.RequestState.FAILED,
        result=object() if terminal_kind == "late_success" else None,
        error=None if terminal_kind == "late_success" else wire_error,
    )

    with pytest.raises(ResponseTimeoutError) as raised:
        actor_module.wait_ticket(ticket)

    assert raised.value is not cleanup_error
    if terminal_kind == "wire_error":
        assert raised.value is wire_error
    assert raised.value.__cause__ is cleanup_error


def test_wait_ticket_surfaces_internal_pin_settlement_error(monkeypatch) -> None:
    broker, lease, _, proxy = _new_fake_proxy(timeout=1)
    proxy._active_call = 1
    proxy._wire_call = 1
    pin_completion = PinCompletion(proxy, 1, time.monotonic() + 1)
    completion = socket_module._TerminalCompletion(pin_completion)
    ticket = actor_module.RequestTicket(
        runtime_epoch=2,
        lease_id=lease.lease_id,
        command=TYPE_SECURITY_COUNT,
        request_payload_snapshot={"market": "sz"},
        deadline=time.monotonic() + 1,
        retry_safe=False,
        completion=completion,
    )
    settlement_error = RuntimeError("pin settlement failed")
    real_terminal = proxy._wire_terminal
    monkeypatch.setattr(
        proxy,
        "_wire_terminal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(settlement_error),
    )
    actor_module._complete_ticket(
        ticket,
        actor_module.RequestState.SUCCESS,
        result=object(),
    )

    with pytest.raises(RuntimeError, match="pin settlement failed") as raised:
        actor_module.wait_ticket(ticket)

    assert raised.value is settlement_error
    assert ticket.completion_error is settlement_error
    monkeypatch.setattr(proxy, "_wire_terminal", real_terminal)
    proxy._wire_terminal(1)
    proxy.close()


def test_pin_completion_returns_before_proxy_condition_and_lazily_advances_fifo() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=4)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)
    proxy = PinnedTransportProxy(
        broker,
        lease,
        socket_module.SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None),
        PushBuffer(1),
        timeout=1,
    )
    proxy._call_counter = 1
    proxy._active_call = 1
    proxy._wire_call = 1
    admitted: list[object] = []
    waiter_thread = threading.Thread(
        target=lambda: admitted.append(proxy._admit(time.monotonic() + 1))
    )
    waiter_thread.start()
    assert broker.wait_for_pin_waiters(1)
    completion = PinCompletion(proxy, 1)
    entered = threading.Event()
    release = threading.Event()

    def hold_condition() -> None:
        with proxy._condition:
            entered.set()
            release.wait()

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert entered.wait(timeout=2)
    done = threading.Event()
    caller = threading.Thread(target=lambda: (completion.publish(None), done.set()))
    caller.start()
    try:
        assert done.wait(timeout=0.2)
    finally:
        release.set()
        holder.join(timeout=2)
        caller.join(timeout=2)
        completion.settle(None)
        waiter_thread.join(timeout=2)
    assert not waiter_thread.is_alive()
    assert admitted == [2]
    assert proxy._active_call == 2
    proxy._wire_terminal(2)
    proxy.close()


def test_pin_publication_wakes_existing_fifo_waiter_for_owner_settlement() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=4)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)
    proxy = PinnedTransportProxy(
        broker,
        lease,
        socket_module.SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None),
        PushBuffer(1),
        timeout=1,
    )
    proxy._call_counter = 1
    proxy._active_call = 1
    proxy._wire_call = 1
    admitted: list[int] = []
    waiter = threading.Thread(target=lambda: admitted.append(proxy._admit(time.monotonic() + 1)))
    waiter.start()
    assert broker.wait_for_pin_waiters(1)

    PinCompletion(proxy, 1).publish(None)
    waiter.join(timeout=0.2)

    assert not waiter.is_alive()
    assert admitted == [2]
    assert proxy._active_call == 2
    proxy._wire_terminal(2)
    proxy.close()


def test_pin_publication_wakes_waiters_without_rejecting_non_head_fifo() -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1, max_pending_requests=4)
    proxy._call_counter = 1
    proxy._active_call = 1
    proxy._wire_call = 1
    results: list[tuple[str, object]] = []

    def admit(label: str) -> None:
        try:
            results.append((label, proxy._admit(time.monotonic() + 1)))
        except BaseException as exc:
            results.append((label, exc))

    first = threading.Thread(target=admit, args=("first",))
    second = threading.Thread(target=admit, args=("second",))
    first.start()
    assert broker.wait_for_pin_waiters(1)
    second.start()
    assert broker.wait_for_pin_waiters(2)
    with proxy._condition:
        assert proxy._condition.wait_for(lambda: len(proxy._waiter_snapshot) == 2, timeout=2)

    PinCompletion(proxy, 1).publish(None)
    first.join(timeout=0.2)
    assert not first.is_alive()
    assert second.is_alive()
    assert results == [("first", 2)]

    proxy._wire_call = 2
    PinCompletion(proxy, 2).publish(None)
    second.join(timeout=0.2)
    assert not second.is_alive()
    assert results == [("first", 2), ("second", 3)]
    proxy._wire_terminal(3)
    proxy.close()


def test_pin_non_head_rechecks_publication_after_clearing_wake(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1, max_pending_requests=4)
    proxy._call_counter = 1
    proxy._active_call = 1
    proxy._wire_call = 1
    real_waiter = pool_module.PinWaiter
    created = 0

    class PublishOnClearEvent(threading.Event):
        armed = False

        def clear(self) -> None:
            if self.armed:
                self.armed = False
                about_to_clear.set()
                assert allow_clear.wait(timeout=2)
            super().clear()

    about_to_clear = threading.Event()
    allow_clear = threading.Event()
    controlled_event = PublishOnClearEvent()

    def waiter_factory(*args, **kwargs):
        nonlocal created
        created += 1
        if created == 2:
            kwargs["completed"] = controlled_event
        return real_waiter(*args, **kwargs)

    monkeypatch.setattr(pool_module, "PinWaiter", waiter_factory)
    results: list[int] = []
    first = threading.Thread(target=lambda: results.append(proxy._admit(time.monotonic() + 1)))
    second = threading.Thread(target=lambda: results.append(proxy._admit(time.monotonic() + 1)))
    first.start()
    assert broker.wait_for_pin_waiters(1)
    second.start()
    assert broker.wait_for_pin_waiters(2)
    with proxy._condition:
        assert proxy._condition.wait_for(lambda: len(proxy._waiter_snapshot) == 2, timeout=2)
    proxy._publish_wire_terminal(1)
    first.join(timeout=0.2)
    assert not first.is_alive()
    assert results == [2]
    assert second.is_alive()
    controlled_event.armed = True
    controlled_event.set()
    assert about_to_clear.wait(timeout=2)
    proxy._wire_call = 2
    proxy._publish_wire_terminal(2)
    allow_clear.set()
    second.join(timeout=0.2)

    assert not first.is_alive() and not second.is_alive()
    assert sorted(results) == [2, 3]
    proxy._wire_terminal(3)
    proxy.close()


def test_pin_old_terminal_settler_preserves_new_publication_before_return(monkeypatch) -> None:
    _, _, _, proxy = _new_fake_proxy(timeout=1, max_pending_requests=2)
    first_call = proxy._admit(time.monotonic() + 1)
    proxy._wire_call = first_call
    queued = PinWaiter(first_call + 1, time.monotonic() + 1)
    with proxy._condition:
        proxy._call_counter = queued.call_id
        proxy._waiters.append(queued)
        proxy._refresh_waiter_snapshot_locked()
    proxy._publish_wire_terminal(first_call)
    real_terminal = proxy._wire_terminal

    def settle_old_then_publish_new(call_id: int, *, deadline=None) -> None:
        real_terminal(call_id, deadline=deadline)
        with proxy._condition:
            next_call = proxy._active_call
            assert next_call is not None and next_call != call_id
            proxy._wire_call = next_call
        proxy._publish_wire_terminal(next_call)

    monkeypatch.setattr(proxy, "_wire_terminal", settle_old_then_publish_new)
    proxy._settle_published_terminal(time.monotonic() + 1)

    assert queued.assigned
    assert queued.completed.is_set()
    assert proxy._active_call == queued.call_id
    assert proxy._active_call_state is not None
    assert proxy._active_call_state.call_id == queued.call_id
    assert proxy._active_call_state.terminal.is_set()

    monkeypatch.setattr(proxy, "_wire_terminal", real_terminal)
    proxy._settle_published_terminal(time.monotonic() + 1)
    assert proxy._active_call is None
    proxy.close()


@pytest.mark.parametrize("publish_after_clear", [False, True], ids=["before-clear", "after-clear"])
def test_pin_old_terminal_clear_rechecks_concurrent_new_publication(publish_after_clear: bool) -> None:
    _, _, _, proxy = _new_fake_proxy(timeout=1)
    clear_window = threading.Event()
    allow_clear = threading.Event()

    class ControlledClearEvent(threading.Event):
        armed = True

        def clear(self) -> None:
            if not self.armed:
                super().clear()
                return
            self.armed = False
            if publish_after_clear:
                super().clear()
            clear_window.set()
            assert allow_clear.wait(timeout=2)
            if not publish_after_clear:
                super().clear()

    publication = ControlledClearEvent()
    proxy._active_call_state = pool_module._PinActiveCall(1, publication)
    proxy._wire_call = 1
    proxy._publish_wire_terminal(1)
    errors: list[BaseException] = []

    def settle() -> None:
        try:
            proxy._settle_published_terminal(time.monotonic() + 1)
        except BaseException as exc:
            errors.append(exc)

    settler = threading.Thread(target=settle)
    settler.start()
    assert clear_window.wait(timeout=2)
    proxy._active_call = 2
    proxy._wire_call = 2
    proxy._publish_wire_terminal(2)
    allow_clear.set()
    settler.join(timeout=2)

    assert not settler.is_alive()
    assert errors == []
    assert proxy._active_call_state is not None
    assert proxy._active_call_state.call_id == 2
    assert proxy._active_call_state.terminal.is_set()
    proxy._settle_published_terminal(time.monotonic() + 1)
    assert proxy._active_call is None
    proxy.close()


def test_pin_exact_old_terminal_publication_clears_without_replay() -> None:
    _, _, _, proxy = _new_fake_proxy(timeout=1)
    proxy._active_call = 1
    state = proxy._active_call_state
    assert state is not None
    proxy._wire_call = 1
    proxy._publish_wire_terminal(1)

    proxy._settle_published_terminal(time.monotonic() + 1)

    assert proxy._active_call is None
    assert not state.terminal.is_set()
    proxy.close()


def test_pin_stale_terminal_timeout_cannot_overwrite_newer_publication(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1, max_pending_requests=2)
    first_call = proxy._admit(time.monotonic() + 1)
    proxy._wire_call = first_call
    queued = PinWaiter(first_call + 1, time.monotonic() + 1)
    with proxy._condition:
        proxy._call_counter = queued.call_id
        proxy._waiters.append(queued)
        proxy._refresh_waiter_snapshot_locked()
    proxy._publish_wire_terminal(first_call)
    old_settler_entered = threading.Event()
    allow_old_settler = threading.Event()
    old_settler_attempting = threading.Event()
    real_terminal = proxy._wire_terminal
    errors: list[BaseException] = []

    def delayed_old_terminal(call_id: int, *, deadline=None) -> None:
        old_settler_entered.set()
        assert allow_old_settler.wait(timeout=2)
        old_settler_attempting.set()
        real_terminal(call_id, deadline=time.monotonic())

    monkeypatch.setattr(proxy, "_wire_terminal", delayed_old_terminal)
    stale_settler = threading.Thread(
        target=lambda: _capture_call(
            lambda: proxy._settle_published_terminal(time.monotonic() + 1),
            errors,
        )
    )
    stale_settler.start()
    assert old_settler_entered.wait(timeout=2)

    real_terminal(first_call, deadline=time.monotonic() + 1)
    assert queued.assigned
    with proxy._condition:
        proxy._wire_call = queued.call_id
        proxy._publish_wire_terminal(queued.call_id)
        allow_old_settler.set()
        assert old_settler_attempting.wait(timeout=2)
    stale_settler.join(timeout=2)

    assert not stale_settler.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], ResponseTimeoutError)
    assert proxy._active_call == queued.call_id

    monkeypatch.setattr(proxy, "_wire_terminal", real_terminal)
    proxy._settle_published_terminal(time.monotonic() + 1)
    assert proxy._active_call is None
    proxy.close()
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.active_leases, snapshot.pin_waiter_count) == (1, 0, 0)


def test_pin_stale_terminal_publish_cannot_replace_newer_publication() -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1, max_pending_requests=2)
    first_call = proxy._admit(time.monotonic() + 1)
    proxy._wire_call = first_call
    queued = PinWaiter(first_call + 1, time.monotonic() + 1)
    with proxy._condition:
        proxy._call_counter = queued.call_id
        proxy._waiters.append(queued)
        proxy._refresh_waiter_snapshot_locked()
    proxy._publish_wire_terminal(first_call)
    real_terminal = proxy._wire_terminal
    real_terminal(first_call, deadline=time.monotonic() + 1)
    with proxy._condition:
        assert proxy._active_call == queued.call_id
        proxy._wire_call = queued.call_id
    proxy._publish_wire_terminal(queued.call_id)
    proxy._publish_wire_terminal(first_call)

    assert proxy._active_call == queued.call_id
    proxy._settle_published_terminal(time.monotonic() + 1)
    assert proxy._active_call is None
    proxy.close()
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.active_leases, snapshot.pin_waiter_count) == (1, 0, 0)


def test_pin_published_terminal_settlement_forwards_absolute_deadline(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1)
    proxy._active_call = 1
    proxy._wire_call = 1
    proxy._publish_wire_terminal(1)
    observed: list[tuple[int, float | None]] = []
    real_terminal = proxy._wire_terminal
    monkeypatch.setattr(
        proxy,
        "_wire_terminal",
        lambda call_id, *, deadline=None: observed.append((call_id, deadline)),
    )
    deadline = time.monotonic() + 0.2

    proxy._settle_published_terminal(deadline)

    assert observed == [(1, deadline)]
    monkeypatch.setattr(proxy, "_wire_terminal", real_terminal)
    proxy._wire_terminal(1)
    proxy.close()


def test_heartbeat_skips_broker_contention_and_actor_still_processes_stop() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    guard = pool_module.HeartbeatAdmissionGuard(broker)
    entered = threading.Event()
    release = threading.Event()

    def hold_condition() -> None:
        with broker._condition:
            entered.set()
            release.wait()

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert entered.wait(timeout=2)
    guard_done = threading.Event()
    guard_results: list[bool] = []
    guard_caller = threading.Thread(
        target=lambda: (guard_results.append(guard()), guard_done.set())
    )
    guard_caller.start()
    try:
        assert guard_done.wait(timeout=0.2)
        assert guard_results == [False]
        runtime = actor_module.start_actor(
            1,
            (),
            heartbeat_interval=0.001,
            heartbeat_allowed=guard,
        )
        tcp_socket = socket.socket()
        generation = actor_module.TcpGeneration(
            1,
            tcp_socket,
            resolve_hosts(["127.0.0.1:9"])[0],
            actor_module.TcpState.READY,
            last_activity_at=time.monotonic() - 1,
        )
        runtime.generation = generation
        actor_module._notify_actor(runtime)
        actor_module.request_actor_stop(runtime)
        assert runtime.stopped.wait(timeout=0.2)
        assert runtime.actor_thread is not None and not runtime.actor_thread.is_alive()
    finally:
        release.set()
        holder.join(timeout=2)
        guard_caller.join(timeout=2)
        if "runtime" in locals() and not runtime.stopped.is_set():
            actor_module.abandon_actor(runtime)


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
        self.wait_deadline: float | None = None

    def wait(self, timeout: float | None = None) -> bool:
        self.wait_deadline = None if timeout is None else time.monotonic() + timeout
        result = self._event.wait(timeout)
        if result:
            self._return_entered.set()
            assert self._allow_return.wait(timeout=2)
        return result

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()


@pytest.mark.parametrize(
    ("platform", "pool_size", "expected"),
    (
        ("linux", 1, (0.0, False)),
        ("linux", 4, (0.0, False)),
        ("win32", 1, (0.0, True)),
        ("win32", 4, (0.002, False)),
    ),
)
def test_actor_cooperation_policy_is_windows_pool_specific(
    monkeypatch, platform: str, pool_size: int, expected: tuple[float, bool]
) -> None:
    monkeypatch.setattr(pool_module.sys, "platform", platform)

    assert pool_module._actor_cooperation(pool_size) == expected


def test_immediate_lease_avoids_event_but_waiter_handoff_keeps_exact_cancellation() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    immediate = broker.acquire(time.monotonic() + 1)
    assert immediate.cancellation is None
    acquired: list[object] = []

    waiter_thread = threading.Thread(
        target=lambda: acquired.append(broker.acquire(time.monotonic() + 2))
    )
    waiter_thread.start()
    assert broker.wait_for_waiters(1)
    with broker._condition:
        waiter_cancellation = broker._waiters[0].cancelled
    assert broker.release(immediate)
    waiter_thread.join(timeout=2)

    assert not waiter_thread.is_alive()
    assert len(acquired) == 1
    queued = acquired[0]
    assert isinstance(queued, pool_module.SlotLease)
    assert queued.cancellation is waiter_cancellation
    assert broker.release(queued)


def test_immediate_lease_release_timeout_is_lazily_reclaimed() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with broker._condition:
            condition_held.set()
            release_condition.wait(timeout=2)

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    completion = pool_module.LeaseCompletion(broker, lease, time.monotonic() + 0.03)
    completion(None)

    assert lease.cancellation is pool_module._CANCELLED_LEASE
    assert lease.cancellation.is_set()
    release_condition.set()
    holder.join(timeout=2)
    assert not holder.is_alive()
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)


def test_broker_abandon_marks_immediate_lease_cancelled() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    assert lease.cancellation is None

    broker.abandon()

    assert lease.cancellation is pool_module._CANCELLED_LEASE
    snapshot = broker.snapshot()
    assert snapshot.closed and snapshot.idle_slots == 0 and snapshot.active_leases == 0


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


def test_broker_condition_lock_obeys_admission_deadline() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with broker._condition:
            condition_held.set()
            release_condition.wait(timeout=0.5)

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    started = time.monotonic()
    try:
        with pytest.raises(ResponseTimeoutError, match="queue"):
            broker.acquire(time.monotonic() + 0.05)
        assert time.monotonic() - started < 0.15
    finally:
        release_condition.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.active_leases) == (1, 0, 0)


def test_broker_delayed_assignment_return_reclaims_expired_lease(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    initial = broker.acquire(time.monotonic() + 1)
    return_entered = threading.Event()
    allow_return = threading.Event()
    completed = DelayedReturnEvent(return_entered, allow_return)
    results: list[object] = []

    waiter = threading.Thread(
        target=lambda: _capture_call(
            lambda: broker.acquire(time.monotonic() + 0.05),
            results,
        )
    )
    monkeypatch.setattr(pool_module.threading, "Event", lambda: completed)
    waiter.start()
    assert broker.wait_for_waiters(1)
    broker.release(initial)
    assert return_entered.wait(timeout=2)
    assert completed.wait_deadline is not None
    while (remaining := completed.wait_deadline - time.monotonic()) > 0:
        time.sleep(remaining)
    allow_return.set()
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


def test_batch_admission_reserves_all_slots_ahead_of_later_single_waiter() -> None:
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=4)
    acquire_many = getattr(broker, "acquire_many", None)
    assert acquire_many is not None, "LeaseBroker must provide atomic batch admission"
    initial = broker.acquire(time.monotonic() + 2)
    batch_leases: list[object] = []
    single_leases: list[object] = []
    batch_done = threading.Event()
    single_done = threading.Event()

    def acquire_batch() -> None:
        batch_leases.extend(acquire_many(2, time.monotonic() + 2))
        batch_done.set()

    def acquire_single() -> None:
        single_leases.append(broker.acquire(time.monotonic() + 2))
        single_done.set()

    batch = threading.Thread(target=acquire_batch)
    single = threading.Thread(target=acquire_single)
    batch.start()
    assert broker.wait_for_waiters(1)
    single.start()
    assert broker.wait_for_waiters(2)

    broker.release(initial)
    assert batch_done.wait(timeout=2)
    assert len(batch_leases) == 2
    assert not single_done.wait(timeout=0.05)
    assert broker.snapshot().active_leases == 2

    for lease in batch_leases:
        broker.release(lease)
    assert single_done.wait(timeout=2)
    assert len(single_leases) == 1
    broker.release(single_leases[0])
    batch.join(timeout=2)
    single.join(timeout=2)
    assert not batch.is_alive() and not single.is_alive()
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.active_leases) == (2, 0, 0)


def test_release_reclaims_concurrent_cancellation_before_batch_assignment(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    first = broker.acquire(time.monotonic() + 2)
    second = broker.acquire(time.monotonic() + 2)
    batch_leases: list[object] = []
    assign_entered = threading.Event()
    allow_assign = threading.Event()
    release_started = threading.Event()
    original_assign = broker._assign_waiters_locked

    def observed_assign(wake) -> None:
        if release_started.is_set():
            assign_entered.set()
            assert allow_assign.wait(timeout=2)
        original_assign(wake)

    monkeypatch.setattr(broker, "_assign_waiters_locked", observed_assign)
    waiter_thread = threading.Thread(
        target=lambda: batch_leases.extend(broker.acquire_many(2, time.monotonic() + 2))
    )
    waiter_thread.start()
    assert broker.wait_for_waiters(1)
    release_started.set()
    release_thread = threading.Thread(target=lambda: broker.release(second))
    release_thread.start()
    assert assign_entered.wait(timeout=2)
    pool_module._mark_lease_cancelled(first)
    allow_assign.set()
    release_thread.join(timeout=2)
    waiter_thread.join(timeout=2)

    assert not release_thread.is_alive() and not waiter_thread.is_alive()
    assert len(batch_leases) == 2
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.active_leases) == (0, 0, 2)
    assert first.state is pool_module.LeaseState.RELEASED
    for lease in batch_leases:
        assert isinstance(lease, pool_module.SlotLease)
        assert broker.release(lease)


def test_batch_admission_timeout_releases_no_partial_lease() -> None:
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    initial = broker.acquire(time.monotonic() + 1)

    with pytest.raises(ResponseTimeoutError, match="during queue"):
        broker.acquire_many(2, time.monotonic() + 0.02)

    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.active_leases) == (1, 0, 1)
    assert broker.release(initial)
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.active_leases) == (2, 0, 0)


def test_batch_admission_close_rejects_entire_waiter() -> None:
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    initial = broker.acquire(time.monotonic() + 1)
    results: list[object] = []

    def acquire_batch() -> None:
        try:
            results.append(broker.acquire_many(2, time.monotonic() + 2))
        except BaseException as exc:
            results.append(exc)

    thread = threading.Thread(target=acquire_batch)
    thread.start()
    assert broker.wait_for_waiters(1)
    broker.close()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(results) == 1 and isinstance(results[0], ConnectionClosedError)
    assert initial.state is pool_module.LeaseState.RELEASED
    snapshot = broker.snapshot()
    assert snapshot.closed
    assert (snapshot.idle_slots, snapshot.waiter_count, snapshot.active_leases) == (0, 0, 0)


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


def test_pool_connect_batch_precedes_later_execute_across_all_slots(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=1, heartbeat_interval=None)
    broker, _ = pool._ensure_started()
    first_pin = broker.acquire(time.monotonic() + 1, pinned=True)
    second_pin = broker.acquire(time.monotonic() + 1, pinned=True)
    allow_connect = threading.Event()
    both_connecting = threading.Event()
    execute_entered = threading.Event()
    connect_results: list[object] = []
    execute_results: list[object] = []
    entered = 0
    entered_lock = threading.Lock()

    def connect_slot(**_kwargs) -> None:
        nonlocal entered
        with entered_lock:
            entered += 1
            if entered == 2:
                both_connecting.set()
        assert allow_connect.wait(timeout=2)

    def execute_slot(*_args, completion, **_kwargs) -> int:
        execute_entered.set()
        completion(None)
        return 123

    for slot in pool._transports:
        monkeypatch.setattr(slot, "_connect_with_deadline", connect_slot)
        monkeypatch.setattr(slot, "_execute_with_lease", execute_slot)

    connect_thread = threading.Thread(
        target=lambda: _capture_call(pool.connect, connect_results),
    )
    execute_thread = threading.Thread(
        target=lambda: _capture_call(
            lambda: pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}),
            execute_results,
        ),
    )
    connect_thread.start()
    assert broker.wait_for_waiters(1)
    execute_thread.start()
    assert broker.wait_for_waiters(2)
    broker.release(first_pin)
    broker.release(second_pin)
    assert both_connecting.wait(timeout=2)
    assert not execute_entered.wait(timeout=0.05)

    allow_connect.set()
    connect_thread.join(timeout=2)
    execute_thread.join(timeout=2)

    assert not connect_thread.is_alive() and not execute_thread.is_alive()
    assert connect_results == [None]
    assert execute_results == [123]
    assert execute_entered.is_set()
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (2, 0)
    pool.close()


def _capture_call(function, results: list[object]) -> None:
    try:
        results.append(function())
    except BaseException as exc:
        results.append(exc)


def _capture_named_call(name: str, function, results: dict[str, object]) -> None:
    try:
        results[name] = function()
    except BaseException as exc:
        results[name] = exc


def test_old_pool_connect_cannot_block_or_clear_reopened_epoch(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    old_broker, _ = pool._ensure_started()
    old_epoch = old_broker.pool_epoch
    slot = pool._transports[0]
    old_entered = threading.Event()
    new_entered = threading.Event()
    allow_old = threading.Event()
    allow_new = threading.Event()
    close_done = threading.Event()
    old_results: list[object] = []
    new_results: list[object] = []
    close_results: list[object] = []

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
    closer = threading.Thread(target=lambda: (_capture_call(pool.close, close_results), close_done.set()))
    closer.start()
    with pool._condition:
        assert pool._condition.wait_for(lambda: pool._state is pool_module.PoolState.CLOSING, timeout=2)
    assert not close_done.wait(timeout=0.05)
    with pytest.raises(ConnectionClosedError, match="CLOSING"):
        pool._ensure_started()
    allow_old.set()
    old_connector.join(timeout=2)
    closer.join(timeout=2)
    assert not old_connector.is_alive() and not closer.is_alive()
    assert len(old_results) == 1 and isinstance(old_results[0], ConnectionClosedError)
    assert close_results == [None]
    assert pool._state is pool_module.PoolState.STOPPED

    new_connector.start()
    try:
        assert new_entered.wait(timeout=2)
        with pool._condition:
            new_broker = pool._broker
            assert new_broker is not None and new_broker is not old_broker
            assert pool._connect_broker is new_broker
        with pool._condition:
            assert pool._connect_broker is new_broker
    finally:
        allow_new.set()
    new_connector.join(timeout=2)

    assert not new_connector.is_alive()
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
        self._submission_gate = threading.Lock()

    def _cancel_lease(self, lease_id: int, **_kwargs) -> None:
        self.cancelled.append(lease_id)


def _new_fake_proxy(timeout: float = 0.05, *, max_pending_requests: int = 4):
    broker = LeaseBroker(2, pool_size=1, max_pending_requests=max_pending_requests)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)
    slot = FakePinnedSlot()
    proxy = PinnedTransportProxy(broker, lease, slot, PushBuffer(2), timeout)
    return broker, lease, slot, proxy


def test_pinned_close_timeout_can_finish_cleanup_and_restore_capacity() -> None:
    broker, lease, slot, proxy = _new_fake_proxy(timeout=0.02)
    proxy._active_call = 1
    proxy._wire_call = 1

    with pytest.raises(TransportCloseTimeoutError, match="close timed out"):
        proxy.close()
    proxy._wire_terminal(1)
    proxy.close()
    proxy.close()

    snapshot = broker.snapshot()
    assert slot.cancelled == [lease.lease_id]
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)


def test_concurrent_pin_close_shares_control_lock_timeout() -> None:
    broker = LeaseBroker(2, pool_size=1, max_pending_requests=4)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)
    slot = socket_module.SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    runtime = actor_module.ActorRuntime(2, ())
    runtime.state = actor_module.RuntimeState.RUNNING
    ticket = actor_module.RequestTicket(
        2,
        lease.lease_id,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
    )
    runtime.active_task = ticket
    slot._runtime = runtime
    proxy = PinnedTransportProxy(broker, lease, slot, PushBuffer(2), timeout=0.05)
    proxy._active_call = 1
    proxy._wire_call = 1
    barrier = threading.Barrier(3)
    done = threading.Event()
    errors: list[BaseException] = []
    result_lock = threading.Lock()
    runtime.control_lock.acquire()

    def close() -> None:
        barrier.wait(timeout=2)
        try:
            proxy.close()
        except BaseException as exc:
            with result_lock:
                errors.append(exc)
                if len(errors) == 2:
                    done.set()

    threads = [threading.Thread(target=close) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=2)
    try:
        assert done.wait(timeout=0.2)
        assert len(errors) == 2
        assert all(isinstance(error, TransportCloseTimeoutError) for error in errors)
        assert len({str(error) for error in errors}) == 1
        assert proxy._state is pool_module.PinState.FAILED
        assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (0, 1)
    finally:
        runtime.control_lock.release()
        for thread in threads:
            thread.join(timeout=2)

    proxy._wire_terminal(1)
    proxy.close()
    assert proxy._state is pool_module.PinState.CLOSED
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)


def test_pin_close_waiter_cleanup_failure_publishes_retryable_attempt(monkeypatch) -> None:
    broker, _, slot, proxy = _new_fake_proxy(timeout=0.1)
    proxy._active_call = 1
    proxy._wire_call = 1
    waiter_done = threading.Event()
    waiter_errors: list[BaseException] = []

    def wait_for_pin() -> None:
        try:
            proxy._admit(time.monotonic() + 1)
        except BaseException as exc:
            waiter_errors.append(exc)
        finally:
            waiter_done.set()

    waiter = threading.Thread(target=wait_for_pin)
    waiter.start()
    assert broker.wait_for_pin_waiters(1)
    original_release = broker.release_pin_waiter
    calls = 0

    def fail_once(completed=None, **kwargs) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("pin waiter cleanup failed")
        original_release(completed, **kwargs)

    monkeypatch.setattr(broker, "release_pin_waiter", fail_once)
    with pytest.raises(RuntimeError, match="pin waiter cleanup failed"):
        proxy.close()

    attempt = proxy._close_attempt
    assert attempt is not None and attempt.completed.is_set()
    assert isinstance(attempt.error, RuntimeError)
    assert proxy._state is pool_module.PinState.FAILED
    assert waiter_done.wait(timeout=2)
    waiter.join(timeout=2)
    assert len(waiter_errors) == 1 and isinstance(waiter_errors[0], ConnectionClosedError)

    cancel_seen = threading.Event()
    original_cancel = slot._cancel_lease

    def observed_cancel(lease_id: int, **kwargs) -> None:
        original_cancel(lease_id, **kwargs)
        cancel_seen.set()

    monkeypatch.setattr(slot, "_cancel_lease", observed_cancel)
    retry_errors: list[BaseException] = []

    def retry_close() -> None:
        try:
            proxy.close()
        except BaseException as exc:
            retry_errors.append(exc)

    retry = threading.Thread(target=retry_close)
    retry.start()
    assert cancel_seen.wait(timeout=2)
    proxy._wire_terminal(1)
    retry.join(timeout=2)

    assert not retry.is_alive()
    assert retry_errors == []
    assert proxy._state is pool_module.PinState.CLOSED
    assert broker.snapshot().pin_waiter_count == 0
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)


def test_failed_pin_completion_retains_cleanup_owner_until_terminal() -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=0.01)
    proxy._active_call = 1
    proxy._wire_call = 1
    completion = PinCompletion(proxy, 1)

    with pytest.raises(TransportCloseTimeoutError, match="close timed out"):
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

    def delayed_release(completed=None, **kwargs) -> None:
        original_release(completed, **kwargs)
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

    def controlled_reserve(completed=None, **kwargs) -> None:
        nonlocal reserve_calls
        with reserve_lock:
            reserve_calls += 1
            call = reserve_calls
        if call == 1:
            first_reserve_entered.set()
            assert allow_first_reserve.wait(timeout=2)
        else:
            second_reserve_entered.set()
        original_reserve(completed, **kwargs)

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


def test_pin_condition_obeys_admission_and_close_deadlines() -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=0.05)
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with proxy._condition:
            condition_held.set()
            release_condition.wait()

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    started = time.monotonic()
    try:
        with pytest.raises(ResponseTimeoutError, match="pinned admission"):
            proxy._admit(time.monotonic() + 0.05)
        assert time.monotonic() - started < 0.15
    finally:
        release_condition.set()
        holder.join(timeout=2)

    condition_held.clear()
    release_condition.clear()
    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    started = time.monotonic()
    try:
        with pytest.raises(TransportCloseTimeoutError, match="state blocked"):
            proxy.close()
        assert time.monotonic() - started < 0.15
        assert proxy._close_requested.is_set()
    finally:
        release_condition.set()
        holder.join(timeout=2)

    with pytest.raises(ConnectionClosedError):
        proxy.request("ping")
    proxy.close()
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)


def test_pin_broker_lock_obeys_admission_and_close_deadlines() -> None:
    broker, lease, _, proxy = _new_fake_proxy(timeout=0.05)
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_broker() -> None:
        with broker._condition:
            condition_held.set()
            release_condition.wait(timeout=0.5)

    holder = threading.Thread(target=hold_broker)
    holder.start()
    assert condition_held.wait(timeout=2)
    started = time.monotonic()
    try:
        with pytest.raises(ResponseTimeoutError):
            proxy._admit(time.monotonic() + 0.05)
        assert time.monotonic() - started < 0.15
    finally:
        release_condition.set()
        holder.join(timeout=2)

    condition_held.clear()
    release_condition.clear()
    holder = threading.Thread(target=hold_broker)
    holder.start()
    assert condition_held.wait(timeout=2)
    started = time.monotonic()
    try:
        with pytest.raises(TransportCloseTimeoutError):
            proxy.close()
        assert time.monotonic() - started < 0.15
        assert lease.state is pool_module.LeaseState.ACTIVE
    finally:
        release_condition.set()
        holder.join(timeout=2)

    proxy.close()
    snapshot = broker.snapshot()
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)


def test_pin_assigned_call_is_withdrawn_when_post_wake_validation_times_out(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=0.2)
    proxy._active_call = 1
    proxy._call_counter = 1
    set_entered = threading.Event()
    allow_set = threading.Event()
    completed = BlockingSetEvent(set_entered, allow_set)
    results: list[object] = []
    broker_held = threading.Event()
    release_broker = threading.Event()
    monkeypatch.setattr(
        pool_module,
        "PinWaiter",
        lambda call_id, deadline, **kwargs: PinWaiter(
            call_id,
            deadline,
            completed=completed,
            **kwargs,
        ),
    )

    waiter = threading.Thread(
        target=lambda: _capture_call(lambda: proxy._admit(time.monotonic() + 0.1), results)
    )
    waiter.start()
    assert broker.wait_for_pin_waiters(1)
    terminal = threading.Thread(target=lambda: proxy._wire_terminal(1))
    terminal.start()
    assert set_entered.wait(timeout=2)

    def hold_broker() -> None:
        with broker._condition:
            broker_held.set()
            release_broker.wait(timeout=0.5)

    holder = threading.Thread(target=hold_broker)
    holder.start()
    assert broker_held.wait(timeout=2)
    allow_set.set()
    waiter.join(timeout=0.3)
    try:
        assert not waiter.is_alive()
        assert len(results) == 1 and isinstance(results[0], ResponseTimeoutError)
        with proxy._condition:
            assert proxy._active_call is None and proxy._wire_call is None
    finally:
        release_broker.set()
        holder.join(timeout=2)
        terminal.join(timeout=2)

    next_call = proxy._admit(time.monotonic() + 0.2)
    proxy._wire_terminal(next_call)
    proxy.close()


def test_pin_post_wake_timeout_hands_off_to_next_live_waiter(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1)
    proxy._active_call = 1
    proxy._call_counter = 1
    set_entered = threading.Event()
    allow_set = threading.Event()
    first_completed = BlockingSetEvent(set_entered, allow_set)
    second_completed = threading.Event()
    completed_events = iter((first_completed, second_completed))
    results: dict[str, object] = {}
    broker_held = threading.Event()
    release_broker = threading.Event()
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

    first = threading.Thread(
        target=lambda: _capture_named_call(
            "first",
            lambda: proxy._admit(time.monotonic() + 0.15),
            results,
        )
    )
    second = threading.Thread(
        target=lambda: _capture_named_call(
            "second",
            lambda: proxy._admit(time.monotonic() + 0.8),
            results,
        )
    )
    first.start()
    assert broker.wait_for_pin_waiters(1)
    second.start()
    assert broker.wait_for_pin_waiters(2)
    terminal = threading.Thread(target=lambda: proxy._wire_terminal(1))
    terminal.start()
    assert set_entered.wait(timeout=2)

    def hold_broker() -> None:
        with broker._condition:
            broker_held.set()
            release_broker.wait(timeout=0.5)

    holder = threading.Thread(target=hold_broker)
    holder.start()
    assert broker_held.wait(timeout=2)
    allow_set.set()
    first.join(timeout=0.35)
    try:
        assert not first.is_alive()
        assert isinstance(results.get("first"), ResponseTimeoutError)
    finally:
        release_broker.set()
        holder.join(timeout=2)
        terminal.join(timeout=2)

    second.join(timeout=2)
    assert not second.is_alive()
    assert results.get("second") == 3
    with proxy._condition:
        assert proxy._active_call == 3 and not proxy._waiters
    proxy._wire_terminal(3)
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
        assert proxy._condition.wait_for(lambda: proxy._state is not pool_module.PinState.OPEN, timeout=2)
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


def test_pin_close_releases_lease_when_assigned_consumer_never_started_wire(monkeypatch) -> None:
    broker, lease, _, proxy = _new_fake_proxy(timeout=0.02)
    proxy._active_call = 1
    proxy._wire_call = 1
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

    proxy.close()
    assert proxy._state is pool_module.PinState.CLOSED
    assert proxy._active_call is None
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)

    allow_return.set()
    waiter.join(timeout=2)
    snapshot = broker.snapshot()

    assert not waiter.is_alive()
    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
    assert proxy._active_call is None
    assert proxy._state is pool_module.PinState.CLOSED
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)
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
        submission_check=None,
    ) -> None:
        with self._submission_gate:
            if submission_check is not None:
                submission_check()
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


@pytest.mark.parametrize("operation", ("connect", "execute"))
def test_pin_close_before_first_wire_submission_rejects_operation(monkeypatch, operation: str) -> None:
    admitted = threading.Event()
    allow_return = threading.Event()
    close_done = threading.Event()
    results: list[object] = []
    close_results: list[object] = []
    broker = LeaseBroker(4, pool_size=1, max_pending_requests=4)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)

    class RecordingSlot(FakePinnedSlot):
        def __init__(self) -> None:
            super().__init__()
            self.submissions = 0

        def _connect_with_deadline(self, *, completion, submission_check=None, **_kwargs) -> None:
            with self._submission_gate:
                if submission_check is not None:
                    submission_check()
                self.submissions += 1
                completion(None)

        def _execute_with_lease(
            self,
            _command,
            _payload,
            *,
            completion,
            submission_check=None,
            **_kwargs,
        ):
            with self._submission_gate:
                if submission_check is not None:
                    submission_check()
                self.submissions += 1
                completion(None)
                return 123

    slot = RecordingSlot()
    proxy = PinnedTransportProxy(broker, lease, slot, PushBuffer(4), timeout=1)
    original_admit = proxy._admit

    def paused_admit(deadline: float) -> int:
        call_id = original_admit(deadline)
        admitted.set()
        assert allow_return.wait(timeout=2)
        return call_id

    monkeypatch.setattr(proxy, "_admit", paused_admit)

    def invoke() -> None:
        try:
            if operation == "connect":
                results.append(proxy.connect())
            else:
                results.append(proxy.execute(TYPE_SECURITY_COUNT, {"market": "sz"}))
        except BaseException as exc:
            results.append(exc)

    def close() -> None:
        try:
            close_results.append(proxy.close())
        except BaseException as exc:
            close_results.append(exc)
        finally:
            close_done.set()

    caller = threading.Thread(target=invoke)
    closer = threading.Thread(target=close)
    caller.start()
    assert admitted.wait(timeout=2)
    closer.start()
    with proxy._condition:
        assert proxy._condition.wait_for(lambda: proxy._state is not pool_module.PinState.OPEN, timeout=2)
    allow_return.set()
    caller.join(timeout=2)
    closer.join(timeout=2)

    assert not caller.is_alive() and not closer.is_alive()
    assert close_done.is_set()
    assert len(results) == 1 and isinstance(results[0], ConnectionClosedError)
    assert close_results == [None]
    assert slot.submissions == 0
    assert proxy._state is pool_module.PinState.CLOSED
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)


def test_pooled_execute_skips_redundant_validation_and_uses_exact_completion(monkeypatch) -> None:
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        for value in (81, 82):
            msg_id, msg_type, _ = read_request(conn)
            conn.sendall(response_bytes(msg_id, msg_type, value.to_bytes(2, "little")))
        release.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport([server.host], pool_size=1, timeout=1, heartbeat_interval=None)
        try:
            pool.connect()
            broker = pool._broker
            assert broker is not None
            original_validate = broker.validate
            monkeypatch.setattr(
                socket_module,
                "_TerminalCompletion",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("redundant wrapper allocated")),
            )
            monkeypatch.setattr(
                broker,
                "validate",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("redundant validation")),
            )

            assert pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 81
            monkeypatch.setattr(broker, "validate", original_validate)
            with pool.pin() as pinned:
                assert pinned.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 82
        finally:
            release.set()
            pool.close()


def test_pooled_execute_relies_on_runtime_guard_after_lease_acquire(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    broker, _ = pool._ensure_started()
    original_acquire = broker.acquire
    lease_acquired = threading.Event()
    allow_return = threading.Event()
    results: list[object] = []

    def paused_acquire(deadline, *, pinned=False):
        lease = original_acquire(deadline, pinned=pinned)
        lease_acquired.set()
        assert allow_return.wait(timeout=2)
        return lease

    monkeypatch.setattr(broker, "acquire", paused_acquire)
    monkeypatch.setattr(
        socket_module,
        "start_actor",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("retired runtime started")),
    )
    caller = threading.Thread(
        target=lambda: _capture_call(
            lambda: pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}),
            results,
        )
    )
    caller.start()
    assert lease_acquired.wait(timeout=2)
    try:
        pool.close()
    finally:
        allow_return.set()
    caller.join(timeout=2)

    assert not caller.is_alive()
    assert len(results) == 1 and isinstance(results[0], ConnectionClosedError)
    snapshot = broker.snapshot()
    assert snapshot.closed and snapshot.active_leases == 0


def test_pinned_proxy_preserves_connection_snapshot_properties() -> None:
    broker, _, slot, proxy = _new_fake_proxy()
    try:
        assert proxy.connected_host == slot.connected_host
        assert proxy.last_handshake == slot.last_handshake
        assert proxy.last_heartbeat == slot.last_heartbeat
    finally:
        proxy.close()
        broker.close()


def test_pinned_proxy_preserves_request_and_push_success_paths(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy()
    first = ResponseFrame(0, 1, 0x0547, 1, 1, b"a", b"a" * 17)
    second = ResponseFrame(0, 2, 0x0547, 1, 1, b"b", b"b" * 17)
    third = ResponseFrame(0, 3, 0x0547, 1, 1, b"c", b"c" * 17)
    fourth = ResponseFrame(0, 4, 0x0547, 1, 1, b"d", b"d" * 17)
    try:
        assert proxy.request("ping") == "pong"
        assert proxy._push_buffer.offer_nowait(PushFrame(2, 1, "127.0.0.1:7709", first))
        assert proxy._push_buffer.offer_nowait(PushFrame(2, 1, "127.0.0.1:7709", second))
        assert proxy.poll_push() is first
        assert proxy.drain_pushes() == [second]

        monkeypatch.setattr(pool_module, "_parse_push", lambda item: ("parsed", item.response.msg_id))
        assert proxy._push_buffer.offer_nowait(PushFrame(2, 1, "127.0.0.1:7709", third))
        assert proxy.poll_push(parse=True) == ("parsed", 3)
        assert proxy._push_buffer.offer_nowait(PushFrame(2, 1, "127.0.0.1:7709", fourth))
        assert proxy.drain_pushes(parse=True) == [("parsed", 4)]
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
    proxy._wire_call = 1
    proxy._call_counter = 1
    first_cancel_entered = threading.Event()
    allow_first_error = threading.Event()
    retry_cancel_entered = threading.Event()
    first_done = threading.Event()
    second_done = threading.Event()
    cancel_calls: list[int] = []
    release_calls = []
    errors: list[BaseException] = []
    original_release = broker.release

    def cancel(lease_id: int, **_kwargs) -> None:
        cancel_calls.append(lease_id)
        if len(cancel_calls) == 1:
            first_cancel_entered.set()
            assert allow_first_error.wait(timeout=2)
            raise OSError("wakeup failed")
        retry_cancel_entered.set()

    def release(item, **kwargs):
        release_calls.append(item)
        return original_release(item, **kwargs)

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
    assert not second_done.wait(timeout=0.05)
    allow_first_error.set()
    assert first_done.wait(timeout=2)
    assert second_done.wait(timeout=2)
    first.join(timeout=2)
    second.join(timeout=2)

    assert len(errors) == 2 and all(isinstance(error, OSError) for error in errors)
    assert {str(error) for error in errors} == {"wakeup failed"}
    assert cancel_calls == [lease.lease_id]
    assert release_calls == []
    assert proxy._state is pool_module.PinState.FAILED

    retry_errors: list[BaseException] = []
    retry = threading.Thread(
        target=lambda: _close_proxy(proxy, retry_errors),
        name="pin-close-explicit-retry",
    )
    retry.start()
    assert retry_cancel_entered.wait(timeout=2)
    proxy._wire_terminal(1)
    retry.join(timeout=2)

    assert not retry.is_alive()
    assert retry_errors == []
    assert cancel_calls == [lease.lease_id, lease.lease_id]
    assert release_calls == [lease]
    assert proxy._state is pool_module.PinState.CLOSED
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)


def _close_proxy(proxy: PinnedTransportProxy, errors: list[BaseException]) -> None:
    try:
        proxy.close()
    except BaseException as exc:
        errors.append(exc)


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


def test_pool_lease_completion_constructor_interrupt_releases_capacity(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    broker, _ = pool._ensure_started()
    monkeypatch.setattr(
        pool_module,
        "LeaseCompletion",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"})

    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)
    pool.close()


def test_pin_completion_constructor_interrupt_clears_active_call(monkeypatch) -> None:
    broker, _, _, proxy = _new_fake_proxy(timeout=1)
    monkeypatch.setattr(
        pool_module,
        "PinCompletion",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    try:
        with pytest.raises(KeyboardInterrupt):
            proxy.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
        assert proxy._active_call is None
    finally:
        if proxy._active_call is not None:
            proxy._wire_terminal(proxy._active_call)
        proxy.close()
        broker.close()


class DeferredTerminalSlot(FakePinnedSlot):
    def __init__(self) -> None:
        super().__init__()
        self.completion = None
        self.cancel_entered = threading.Event()

    def _execute_with_lease(self, command, payload, **kwargs):
        with self._submission_gate:
            submission_check = kwargs.get("submission_check")
            if submission_check is not None:
                submission_check()
            self.completion = kwargs["completion"]
            raise ResponseTimeoutError("deferred Actor terminal")

    def _cancel_lease(self, lease_id: int, **_kwargs) -> None:
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


def test_broker_admission_interrupt_removes_waiter_without_losing_capacity(monkeypatch) -> None:
    class InterruptingEvent:
        def __init__(self) -> None:
            self._event = pool_module._INTERNAL_EVENT()

        def wait(self, _timeout: float | None = None) -> bool:
            raise KeyboardInterrupt

        def set(self) -> None:
            self._event.set()

        def is_set(self) -> bool:
            return self._event.is_set()

    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    initial = broker.acquire(time.monotonic() + 1)
    monkeypatch.setattr(pool_module.threading, "Event", InterruptingEvent)

    with pytest.raises(KeyboardInterrupt):
        broker.acquire(time.monotonic() + 1)

    queued = broker.snapshot()
    assert (queued.idle_slots, queued.waiter_count, queued.active_leases) == (0, 0, 1)
    broker.release(initial)
    released = broker.snapshot()
    assert (released.idle_slots, released.waiter_count, released.active_leases) == (1, 0, 0)


def test_pin_admission_interrupt_removes_waiter_and_reservation(monkeypatch) -> None:
    broker, _lease, _slot, proxy = _new_fake_proxy(timeout=1)
    first_call = proxy._admit(time.monotonic() + 1)
    real_pin_waiter = pool_module.PinWaiter

    def interrupting_pin_waiter(*args, **kwargs):
        waiter = real_pin_waiter(*args, **kwargs)
        monkeypatch.setattr(
            waiter.completed,
            "wait",
            lambda _timeout=None: (_ for _ in ()).throw(KeyboardInterrupt()),
        )
        return waiter

    monkeypatch.setattr(pool_module, "PinWaiter", interrupting_pin_waiter)
    with pytest.raises(KeyboardInterrupt):
        proxy._admit(time.monotonic() + 1)

    assert proxy._active_call == first_call
    assert not proxy._waiters
    assert broker.snapshot().pin_waiter_count == 0
    proxy._wire_terminal(first_call)
    assert proxy._active_call is None
    proxy.close()
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)


def test_pin_close_lock_timeout_then_wire_terminal_releases_lease() -> None:
    broker, lease, _slot, proxy = _new_fake_proxy(timeout=0.03)
    proxy._active_call = 1
    proxy._wire_call = 1
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with proxy._condition:
            condition_held.set()
            release_condition.wait()

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    try:
        with pytest.raises(TransportCloseTimeoutError, match="state blocked"):
            proxy.close()
    finally:
        release_condition.set()
        holder.join(timeout=2)

    proxy._wire_terminal(1)
    proxy.close()
    snapshot = broker.snapshot()
    assert lease.state is pool_module.LeaseState.RELEASED
    assert proxy._state is pool_module.PinState.CLOSED
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)


def test_failed_pin_terminal_lazily_reclaims_lease_when_broker_is_contended() -> None:
    broker, lease, _slot, proxy = _new_fake_proxy(timeout=0.03)
    proxy._active_call = 1
    proxy._wire_call = 1
    proxy._state = pool_module.PinState.FAILED
    broker_held = threading.Event()
    release_broker = threading.Event()

    def hold_broker() -> None:
        with broker._condition:
            broker_held.set()
            assert release_broker.wait(timeout=2)

    holder = threading.Thread(target=hold_broker)
    holder.start()
    assert broker_held.wait(timeout=2)
    terminal_done = threading.Event()
    terminal = threading.Thread(
        target=lambda: (proxy._wire_terminal(1), terminal_done.set())
    )
    terminal.start()
    assert terminal_done.wait(timeout=0.2)
    terminal.join(timeout=0.2)
    assert not terminal.is_alive()
    release_broker.set()
    holder.join(timeout=2)

    assert not terminal.is_alive() and not holder.is_alive()
    snapshot = broker.snapshot()
    assert lease.state is pool_module.LeaseState.RELEASED
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)
    replacement = broker.acquire(time.monotonic() + 1, pinned=True)
    assert broker.validate(replacement)
    proxy.close()
    assert broker.validate(replacement)
    replacement_snapshot = broker.snapshot()
    assert (replacement_snapshot.idle_slots, replacement_snapshot.active_leases) == (0, 1)
    broker.release(replacement)


def test_assigned_pin_waiter_release_failure_is_lazily_reclaimed(monkeypatch) -> None:
    broker, _lease, _slot, proxy = _new_fake_proxy(timeout=0.1, max_pending_requests=1)
    first_call = proxy._admit(time.monotonic() + 1)
    proxy._wire_call = first_call
    results: list[object] = []
    waiter_deadline = time.monotonic() + 1
    release_calls: list[threading.Event] = []
    second_release_entered = threading.Event()
    allow_second_release = threading.Event()
    original_release_pin_waiter = broker.release_pin_waiter

    def controlled_release_pin_waiter(completed, *, deadline=None) -> None:
        release_calls.append(completed)
        if len(release_calls) == 1:
            raise TransportCloseTimeoutError("terminal pin reservation release blocked")
        if len(release_calls) == 2:
            second_release_entered.set()
            assert allow_second_release.wait(timeout=2)
            raise TransportCloseTimeoutError("assigned pin reservation release blocked")
        original_release_pin_waiter(completed, deadline=deadline)

    monkeypatch.setattr(broker, "release_pin_waiter", controlled_release_pin_waiter)
    waiter = threading.Thread(
        target=lambda: _capture_call(lambda: proxy._admit(waiter_deadline), results)
    )
    waiter.start()
    assert broker.wait_for_pin_waiters(1)
    terminal_done = threading.Event()
    terminal = threading.Thread(
        target=lambda: (proxy._wire_terminal(first_call), terminal_done.set())
    )
    terminal.start()
    assert terminal_done.wait(timeout=0.2)
    assert second_release_entered.wait(timeout=0.2)
    with proxy._condition:
        assigned_waiter = proxy._active_waiter
        assert assigned_waiter is not None
        assert assigned_waiter.assigned
        assert proxy._active_call == assigned_waiter.call_id
    assert time.monotonic() < waiter_deadline
    allow_second_release.set()
    waiter.join(timeout=1)
    terminal.join(timeout=2)

    assert not waiter.is_alive() and not terminal.is_alive()
    assert len(release_calls) == 2
    assert len(results) == 1 and isinstance(results[0], TransportCloseTimeoutError)
    with proxy._condition:
        assert proxy._active_call is None
        assert not proxy._waiters
        assert proxy._waiter_snapshot == ()
    admitted: list[object] = []
    normal = threading.Thread(
        target=lambda: _capture_call(
            lambda: broker.acquire(time.monotonic() + 1),
            admitted,
        )
    )
    normal.start()
    assert broker.wait_for_waiters(1, timeout=0.2)
    proxy.close()
    normal.join(timeout=2)

    assert not normal.is_alive()
    assert len(admitted) == 1 and isinstance(admitted[0], pool_module.SlotLease)
    broker.release(admitted[0])
    snapshot = broker.snapshot()
    assert snapshot.pin_waiter_count == 0
    assert (snapshot.idle_slots, snapshot.active_leases) == (1, 0)


def test_assigned_pin_waiter_interrupt_is_lazily_reaped_after_cleanup_timeout(monkeypatch) -> None:
    broker, _lease, _slot, proxy = _new_fake_proxy(timeout=0.2)
    first_call = proxy._admit(time.monotonic() + 1)
    proxy._wire_call = first_call
    assigned = threading.Event()
    holder_ready = threading.Event()
    release_holder = threading.Event()
    inner = threading.Event()
    real_pin_waiter = pool_module.PinWaiter
    results: list[object] = []

    class AssignedInterruptEvent:
        def wait(self, timeout=None) -> bool:
            assert inner.wait(timeout=timeout)
            raise KeyboardInterrupt

        def set(self) -> None:
            assigned.set()
            assert holder_ready.wait(timeout=2)
            inner.set()

        def is_set(self) -> bool:
            return inner.is_set()

    def make_waiter(*args, **kwargs):
        waiter = real_pin_waiter(*args, **kwargs)
        waiter.completed = AssignedInterruptEvent()
        return waiter

    def hold_condition() -> None:
        assert assigned.wait(timeout=2)
        with proxy._condition:
            holder_ready.set()
            release_holder.wait()

    monkeypatch.setattr(pool_module, "PinWaiter", make_waiter)
    holder = threading.Thread(target=hold_condition)
    holder.start()
    waiter = threading.Thread(target=lambda: _capture_call(lambda: proxy._admit(time.monotonic() + 0.08), results))
    waiter.start()
    assert broker.wait_for_pin_waiters(1)
    terminal = threading.Thread(target=proxy._wire_terminal, args=(first_call,))
    terminal.start()
    waiter.join(timeout=0.3)
    try:
        assert not waiter.is_alive()
        assert len(results) == 1 and isinstance(results[0], KeyboardInterrupt)
    finally:
        release_holder.set()
        holder.join(timeout=2)
        terminal.join(timeout=2)

    monkeypatch.setattr(pool_module, "PinWaiter", real_pin_waiter)
    replacement = proxy._admit(time.monotonic() + 0.2)
    assert replacement > first_call
    proxy._wire_terminal(replacement)
    proxy.close()
    assert (broker.snapshot().idle_slots, broker.snapshot().active_leases) == (1, 0)
