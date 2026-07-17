from __future__ import annotations

import threading
import time
import weakref
from collections import deque

import pytest

from eltdx.exceptions import ConnectionClosedError, TransportCloseTimeoutError
from eltdx.protocol.frame import ResponseFrame
from eltdx.transport import pool as pool_module
from eltdx.transport import push as push_module
from eltdx.transport.actor import ActorRuntime
from eltdx.transport.pool import (
    AdmissionState,
    AdmissionWaiter,
    LeaseBroker,
    LeaseState,
    PinnedTransportProxy,
    PoolRuntimeGuard,
)
from eltdx.transport.push import PushBuffer, PushFrame


_REAL_EVENT = threading.Event


def _bind_broker_retirement(broker: LeaseBroker, retire: threading.Event) -> None:
    broker._retire_event = retire
    if not hasattr(broker, "_close_requested"):
        broker._close_requested = _REAL_EVENT()


def _bind_push_retirement(push: PushBuffer, retire: threading.Event) -> None:
    push._retire_event = retire
    if not hasattr(push, "_close_requested"):
        push._close_requested = _REAL_EVENT()


def _frame(epoch: int, msg_id: int = 1) -> PushFrame:
    raw = b"x" * 17
    response = ResponseFrame(0, msg_id, 0x0547, 1, 1, b"x", raw)
    return PushFrame(epoch, 1, "127.0.0.1:7709", response)


class _RetireOnPopleft(deque):
    def __init__(self, values, retire: threading.Event) -> None:
        super().__init__(values)
        self._retire = retire

    def popleft(self):
        item = super().popleft()
        self._retire.set()
        return item


class _RetireOnAppend(deque):
    def __init__(self, values, retire: threading.Event) -> None:
        super().__init__(values)
        self._retire = retire

    def append(self, item) -> None:
        super().append(item)
        self._retire.set()


class _RetireOnSet(dict):
    def __init__(self, values, retire: threading.Event) -> None:
        super().__init__(values)
        self._retire = retire

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        self._retire.set()


class _RetireOnAdd(set):
    def __init__(self, values, retire: threading.Event) -> None:
        super().__init__(values)
        self._retire = retire

    def add(self, item) -> None:
        super().add(item)
        self._retire.set()


class _MustBeSignalledEvent:
    def __init__(self) -> None:
        self._event = _REAL_EVENT()

    def set(self) -> None:
        self._event.set()

    def clear(self) -> None:
        self._event.clear()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, _timeout: float | None = None) -> bool:
        assert self._event.is_set(), "permanent retirement wake was lost"
        return True


class _ClearRetiresEvent(_MustBeSignalledEvent):
    def __init__(self, retire: threading.Event) -> None:
        super().__init__()
        self._retire = retire
        self.clear_calls = 0

    def clear(self) -> None:
        self.clear_calls += 1
        super().clear()
        self._retire.set()


@pytest.mark.parametrize("count", (1, 2))
def test_immediate_and_batch_lease_publication_roll_back_after_retirement(count: int) -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    _bind_broker_retirement(broker, retire)
    broker._idle_slots = _RetireOnPopleft(broker._idle_slots, retire)

    with pytest.raises(ConnectionClosedError):
        if count == 1:
            broker.acquire(time.monotonic() + 1)
        else:
            broker.acquire_many(count, time.monotonic() + 1)

    snapshot = broker.snapshot()
    assert snapshot.closed
    assert (snapshot.idle_slots, snapshot.active_leases) == (0, 0)


def test_waiter_insertion_rechecks_retirement_and_wakes_without_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    _bind_broker_retirement(broker, retire)
    active = broker.acquire(time.monotonic() + 1)
    broker._waiters = _RetireOnAppend(broker._waiters, retire)
    monkeypatch.setattr(pool_module.threading, "Event", _MustBeSignalledEvent)

    with pytest.raises(ConnectionClosedError):
        broker.acquire(time.monotonic() + 1)

    assert active.state is LeaseState.RELEASED
    snapshot = broker.snapshot()
    assert snapshot.closed and snapshot.waiter_count == 0 and snapshot.active_leases == 0


def test_waiter_assignment_rolls_back_lease_when_retirement_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    _bind_broker_retirement(broker, retire)
    active = broker.acquire(time.monotonic() + 1)
    waiter = AdmissionWaiter(1, 99, time.monotonic() + 1, False)
    with broker._condition:
        broker._waiters.append(waiter)
        broker._waiter_snapshot = (waiter,)
    real_new_lease = broker._new_lease_locked

    def retire_after_new_lease(*args, **kwargs):
        lease = real_new_lease(*args, **kwargs)
        retire.set()
        return lease

    monkeypatch.setattr(broker, "_new_lease_locked", retire_after_new_lease)
    broker.release(active)

    assert waiter.state is AdmissionState.CLOSED
    assert waiter.completed.is_set()
    assert waiter.assigned_leases
    assert all(lease.state is LeaseState.RELEASED for lease in waiter.assigned_leases)
    snapshot = broker.snapshot()
    assert snapshot.closed and snapshot.active_leases == 0 and snapshot.idle_slots == 0


def test_waiter_clear_rechecks_permanent_retirement(monkeypatch: pytest.MonkeyPatch) -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    _bind_broker_retirement(broker, retire)
    active = broker.acquire(time.monotonic() + 1)
    controlled = _ClearRetiresEvent(retire)
    controlled.set()
    monkeypatch.setattr(pool_module.threading, "Event", lambda: controlled)

    with pytest.raises(ConnectionClosedError):
        broker.acquire(time.monotonic() + 1)

    assert controlled.clear_calls == 1
    assert active.state is LeaseState.RELEASED


def test_pin_reservation_rolls_back_when_retirement_wins() -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    _bind_broker_retirement(broker, retire)
    completed = _REAL_EVENT()
    broker._pin_waiter_events = _RetireOnSet(broker._pin_waiter_events, retire)

    with pytest.raises(ConnectionClosedError):
        broker.reserve_pin_waiter(completed, cancelled=_REAL_EVENT())

    assert completed.is_set()
    snapshot = broker.snapshot()
    assert snapshot.closed and snapshot.pin_waiter_count == 0


@pytest.mark.parametrize("action", ("release", "reclaim"))
def test_release_and_reclaim_never_republish_idle_capacity_after_retirement(action: str) -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    _bind_broker_retirement(broker, retire)
    lease = broker.acquire(time.monotonic() + 1)
    retire.set()
    if action == "release":
        broker.release(lease)
    else:
        if lease.cancellation is None:
            lease.cancellation = _REAL_EVENT()
        assert lease.cancellation is not None
        lease.cancellation.set()

    snapshot = broker.snapshot()
    assert snapshot.closed
    assert (snapshot.idle_slots, snapshot.active_leases) == (0, 0)
    assert lease.state is LeaseState.RELEASED


def test_heartbeat_rejects_retired_epoch() -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    _bind_broker_retirement(broker, retire)
    retire.set()

    assert not broker.allows_heartbeat()


def test_close_after_abandon_retries_full_broker_drain() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)

    broker.abandon()
    broker.close()

    assert lease.state is LeaseState.RELEASED
    assert lease.cancellation is not None and lease.cancellation.is_set()
    assert broker._active_leases == {}
    snapshot = broker.snapshot()
    assert snapshot.closed and snapshot.active_leases == 0 and snapshot.idle_slots == 0


def test_broker_close_timeout_can_be_retried_to_full_drain() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)
    held = _REAL_EVENT()
    release = _REAL_EVENT()

    def hold_condition() -> None:
        with broker._condition:
            held.set()
            assert release.wait(timeout=2)

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert held.wait(timeout=2)
    try:
        with pytest.raises(TransportCloseTimeoutError):
            broker.close(deadline=time.monotonic() + 0.02)
    finally:
        release.set()
        holder.join(timeout=2)

    broker.close()
    snapshot = broker.snapshot()
    assert lease.state is LeaseState.RELEASED
    assert snapshot.closed and snapshot.active_leases == 0 and snapshot.idle_slots == 0


def test_push_append_is_rolled_back_when_retirement_wins() -> None:
    retire = _REAL_EVENT()
    push = PushBuffer(1)
    _bind_push_retirement(push, retire)
    push._frames = _RetireOnAppend(push._frames, retire)

    assert not push.offer_nowait(_frame(1))
    assert not push._frames and push._bytes == 0


def test_push_poll_registration_rechecks_permanent_retirement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retire = _REAL_EVENT()
    push = PushBuffer(1)
    _bind_push_retirement(push, retire)
    push._waiters = _RetireOnAdd(push._waiters, retire)
    monkeypatch.setattr(push_module.threading, "Event", _MustBeSignalledEvent)

    assert push.poll(1) is None


def test_push_poll_clear_rechecks_permanent_retirement(monkeypatch: pytest.MonkeyPatch) -> None:
    retire = _REAL_EVENT()
    push = PushBuffer(1)
    _bind_push_retirement(push, retire)
    events: list[_ClearRetiresEvent] = []

    def event_factory() -> _ClearRetiresEvent:
        event = _ClearRetiresEvent(retire)
        events.append(event)
        return event

    monkeypatch.setattr(push_module.threading, "Event", event_factory)

    assert push.poll(1) is None
    assert len(events) == 1 and events[0].clear_calls == 1


def test_pin_waiter_clear_rechecks_broker_retirement(monkeypatch: pytest.MonkeyPatch) -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    _bind_broker_retirement(broker, retire)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)
    proxy = PinnedTransportProxy(broker, lease, object(), PushBuffer(1), timeout=1)
    first_call = proxy._admit(time.monotonic() + 1)
    controlled = _ClearRetiresEvent(retire)
    controlled.set()
    real_pin_waiter = pool_module.PinWaiter

    def make_waiter(call_id: int, deadline: float):
        return real_pin_waiter(call_id, deadline, completed=controlled)

    monkeypatch.setattr(pool_module, "PinWaiter", make_waiter)

    with pytest.raises(ConnectionClosedError):
        proxy._admit(time.monotonic() + 1)

    assert controlled.clear_calls == 1
    proxy._wire_terminal(first_call)
    broker.close()


def test_fatal_error_preempts_buffered_push_frame() -> None:
    retire = _REAL_EVENT()
    push = PushBuffer(1)
    _bind_push_retirement(push, retire)
    assert push.offer_nowait(_frame(1))
    error = RuntimeError("fatal push")

    retire.set()
    push.publish_close(error)

    with pytest.raises(RuntimeError, match="fatal push") as raised:
        push.poll()
    assert raised.value is error


def test_push_close_timeout_can_be_retried_to_clear_residual_frames() -> None:
    push = PushBuffer(1)
    assert push.offer_nowait(_frame(1))
    held = _REAL_EVENT()
    release = _REAL_EVENT()

    def hold_condition() -> None:
        with push._condition:
            held.set()
            assert release.wait(timeout=2)

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert held.wait(timeout=2)
    error = RuntimeError("fatal push close")
    try:
        push.abandon(error)
        with pytest.raises(TransportCloseTimeoutError):
            push.close_before_deadline(time.monotonic() + 0.02, error)
    finally:
        release.set()
        holder.join(timeout=2)

    push.close(error)
    snapshot = push.snapshot()
    assert snapshot.closed and snapshot.frame_count == 0 and snapshot.byte_count == 0


def test_failure_cell_contention_cannot_gate_fatal_fanout() -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    push = PushBuffer(1)
    _bind_broker_retirement(broker, retire)
    _bind_push_retirement(push, retire)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push, retire_event=retire)
    failed = ActorRuntime(1, ())
    sibling = ActorRuntime(1, ())
    assert guard.add_runtime(failed, pool_epoch=1, broker=broker)
    assert guard.add_runtime(sibling, pool_epoch=1, broker=broker)
    handle = pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
    error = RuntimeError("fatal under failure-cell contention")
    failed.fatal_error = error

    with guard._failure_cell.lock:
        handle.publish(failed, error)
        assert guard.failure() is error
        assert failed.stop_requested and sibling.stop_requested

    assert retire.is_set()
    assert broker.snapshot().closed
    assert push.snapshot().closed


def test_two_actor_fatals_each_complete_epoch_fanout() -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    push = PushBuffer(1)
    _bind_broker_retirement(broker, retire)
    _bind_push_retirement(push, retire)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push, retire_event=retire)
    runtimes = (ActorRuntime(1, ()), ActorRuntime(1, ()))
    for runtime in runtimes:
        assert guard.add_runtime(runtime, pool_epoch=1, broker=broker)
    handles = tuple(
        pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
        for _ in runtimes
    )
    errors = (RuntimeError("first fatal"), RuntimeError("second fatal"))
    for runtime, error in zip(runtimes, errors):
        runtime.fatal_error = error
    barrier = threading.Barrier(3)

    def publish(index: int) -> None:
        barrier.wait(timeout=2)
        handles[index].publish(runtimes[index], errors[index])

    threads = [threading.Thread(target=publish, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=2)
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert all(handle._published.is_set() for handle in handles)
    assert all(runtime.stop_requested for runtime in runtimes)
    assert broker.snapshot().closed and push.snapshot().closed


def test_delayed_old_epoch_fatal_does_not_retire_new_epoch() -> None:
    guard = PoolRuntimeGuard()
    old_retire = _REAL_EVENT()
    old_broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    old_push = PushBuffer(1)
    _bind_broker_retirement(old_broker, old_retire)
    _bind_push_retirement(old_push, old_retire)
    guard.configure(old_broker, old_push, retire_event=old_retire)
    old_runtime = ActorRuntime(1, ())
    old_handle = pool_module.ActorFatalHandle(
        weakref.ref(guard), 1, weakref.ref(old_broker), old_retire
    )
    allow_old_fatal = _REAL_EVENT()
    old_done = _REAL_EVENT()

    def publish_old() -> None:
        assert allow_old_fatal.wait(timeout=2)
        old_handle.publish(old_runtime, RuntimeError("delayed old fatal"))
        old_done.set()

    thread = threading.Thread(target=publish_old)
    thread.start()
    new_retire = _REAL_EVENT()
    new_broker = LeaseBroker(2, pool_size=1, max_pending_requests=1)
    new_push = PushBuffer(2)
    _bind_broker_retirement(new_broker, new_retire)
    _bind_push_retirement(new_push, new_retire)
    guard.configure(new_broker, new_push, retire_event=new_retire)
    allow_old_fatal.set()
    assert old_done.wait(timeout=2)
    thread.join(timeout=2)

    assert old_retire.is_set() and old_runtime.stop_requested
    assert not new_retire.is_set()
    assert not new_broker.snapshot().closed
    assert not new_push.snapshot().closed
