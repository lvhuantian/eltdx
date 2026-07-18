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


class _RetireOnSecondAppend(deque):
    def __init__(self, values, retire: threading.Event) -> None:
        super().__init__(values)
        self._retire = retire
        self._append_count = 0

    def append(self, item) -> None:
        super().append(item)
        self._append_count += 1
        if self._append_count == 2:
            self._retire.set()


class _PauseBeforePopleft(deque):
    def __init__(self, values, entered: threading.Event, allow_read: threading.Event) -> None:
        super().__init__(values)
        self._entered = entered
        self._allow_read = allow_read

    def popleft(self):
        self._entered.set()
        assert self._allow_read.wait(timeout=2)
        return super().popleft()


class _PauseBeforeIter(deque):
    def __init__(self, values, entered: threading.Event, allow_read: threading.Event) -> None:
        super().__init__(values)
        self._entered = entered
        self._allow_read = allow_read

    def __iter__(self):
        self._entered.set()
        assert self._allow_read.wait(timeout=2)
        return super().__iter__()


class _RetireOnSet(dict):
    def __init__(self, values, retire: threading.Event) -> None:
        super().__init__(values)
        self._retire = retire

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        self._retire.set()


class _RetireOnValues(dict):
    def __init__(self, values, retire: threading.Event) -> None:
        super().__init__(values)
        self._retire = retire

    def values(self):
        self._retire.set()
        return super().values()


class _RetireOnGet(dict):
    def __init__(self, values, retire: threading.Event) -> None:
        super().__init__(values)
        self._retire = retire

    def get(self, key, default=None):
        value = super().get(key, default)
        self._retire.set()
        return value


class _RetireOnDelete(dict):
    def __init__(self, values, retire: threading.Event) -> None:
        super().__init__(values)
        self._retire = retire

    def __delitem__(self, key) -> None:
        super().__delitem__(key)
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


class _FirstSetPausesEvent:
    def __init__(self, entered: threading.Event, allow_first: threading.Event) -> None:
        self._event = _REAL_EVENT()
        self._entered = entered
        self._allow_first = allow_first
        self._lock = threading.Lock()
        self._set_calls = 0

    def set(self) -> None:
        with self._lock:
            self._set_calls += 1
            first = self._set_calls == 1
        if first:
            self._entered.set()
            assert self._allow_first.wait(timeout=2)
        self._event.set()

    def clear(self) -> None:
        self._event.clear()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


class _SetThenPauseEvent:
    def __init__(self, entered: threading.Event, allow_set: threading.Event) -> None:
        self._event = _REAL_EVENT()
        self._entered = entered
        self._allow_set = allow_set

    def set(self) -> None:
        self._event.set()
        self._entered.set()
        assert self._allow_set.wait(timeout=2)

    def clear(self) -> None:
        self._event.clear()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


class _PauseFirstFatalPublishBuffer(PushBuffer):
    """Pause one fatal publisher after it observes an empty error slot."""

    def __init__(self, owner_epoch: int) -> None:
        super().__init__(owner_epoch)
        self.read_entered = _REAL_EVENT()
        self.allow_read = _REAL_EVENT()
        self._pause_armed = True

    def __getattribute__(self, name: str):
        value = super().__getattribute__(name)
        if name != "_published_error" or value is not None:
            return value
        try:
            armed = super().__getattribute__("_pause_armed")
        except AttributeError:
            return value
        if armed and threading.current_thread().name == "first-fatal-publisher":
            super().__setattr__("_pause_armed", False)
            self.read_entered.set()
            assert self.allow_read.wait(timeout=2)
        return value


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
    broker._idle_slots = _RetireOnAppend(broker._idle_slots, retire)
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
    lease = broker.acquire(time.monotonic() + 1)
    broker._active_leases = _RetireOnValues(broker._active_leases, retire)

    assert not broker.allows_heartbeat()
    assert broker._closed and broker._drained
    assert not broker._active_leases
    assert lease.state is LeaseState.RELEASED


def test_validate_drains_when_retirement_wins_final_recheck() -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    _bind_broker_retirement(broker, retire)
    lease = broker.acquire(time.monotonic() + 1)
    broker._active_leases = _RetireOnGet(broker._active_leases, retire)

    assert not broker.validate(lease)
    assert broker._closed and broker._drained
    assert not broker._active_leases
    assert lease.state is LeaseState.RELEASED


@pytest.mark.parametrize("cancelled_before_release", (False, True))
def test_pin_release_and_reclaim_drain_retirement_critical_window(
    cancelled_before_release: bool,
) -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    _bind_broker_retirement(broker, retire)
    lease = broker.acquire(time.monotonic() + 1)
    completed = _REAL_EVENT()
    cancelled = _REAL_EVENT()
    broker.reserve_pin_waiter(completed, cancelled=cancelled)
    broker._pin_waiter_events = _RetireOnDelete(broker._pin_waiter_events, retire)
    if cancelled_before_release:
        cancelled.set()

    broker.release_pin_waiter(completed)

    assert broker._closed and broker._drained
    assert not broker._active_leases
    assert lease.state is LeaseState.RELEASED


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


def test_push_rollback_removes_exact_new_frame_not_equal_predecessor() -> None:
    retire = _REAL_EVENT()
    push = PushBuffer(1)
    _bind_push_retirement(push, retire)
    push._frames = _RetireOnSecondAppend(push._frames, retire)
    first = _frame(1)
    second = _frame(1)
    assert first == second and first is not second

    assert push.offer_nowait(first)
    assert not push.offer_nowait(second)

    assert len(push._frames) == 1 and push._frames[0] is first


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


@pytest.mark.parametrize("operation", ("poll", "drain"))
def test_fatal_during_push_read_preempts_buffered_frame(operation: str) -> None:
    retire = _REAL_EVENT()
    push = PushBuffer(1)
    _bind_push_retirement(push, retire)
    assert push.offer_nowait(_frame(1))
    read_entered = _REAL_EVENT()
    allow_read = _REAL_EVENT()
    container_type = _PauseBeforePopleft if operation == "poll" else _PauseBeforeIter
    push._frames = container_type(push._frames, read_entered, allow_read)
    error = RuntimeError(f"fatal during push {operation}")
    outcome: list[object] = []

    def read() -> None:
        try:
            outcome.append(push.poll() if operation == "poll" else push.drain())
        except BaseException as exc:
            outcome.append(exc)

    reader = threading.Thread(target=read)
    reader.start()
    assert read_entered.wait(timeout=2)
    retire.set()
    push.abandon(error)
    allow_read.set()
    reader.join(timeout=2)

    assert not reader.is_alive()
    assert outcome == [error]
    assert push._closed and push._drained
    assert not push._frames and push._bytes == 0


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


def test_failure_cell_contention_recovers_unregistered_handle_fatal() -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    push = PushBuffer(1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push, retire_event=retire)
    handle = pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
    runtime = ActorRuntime(1, ())
    error = RuntimeError("fatal before runtime registration")
    runtime.fatal_error = error

    with guard._failure_cell.lock:
        handle.publish(runtime, error)
        assert guard.failure() is error

    assert not guard._runtime_snapshot
    assert handle._published.is_set() and handle._error is error


def test_pre_registration_fatal_reason_precedes_observable_retirement() -> None:
    set_entered = _REAL_EVENT()
    allow_set = _REAL_EVENT()
    retire = _SetThenPauseEvent(set_entered, allow_set)
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    push = PushBuffer(1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push, retire_event=retire)
    handle = pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
    runtime = ActorRuntime(1, ())
    error = RuntimeError("fatal while retirement publication pauses")
    runtime.fatal_error = error
    publisher = threading.Thread(target=lambda: handle.publish(runtime, error))
    publisher.start()
    assert set_entered.wait(timeout=2)

    try:
        assert guard.failure() is error
        assert guard.finish_epoch(pool_epoch=1, broker=broker) is error
    finally:
        allow_set.set()
        publisher.join(timeout=2)

    assert not publisher.is_alive()
    assert handle._published.is_set() and handle._error is error


def test_fatal_force_wake_does_not_trust_in_progress_normal_publication() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=2)
    active = broker.acquire(time.monotonic() + 1)
    set_entered = _REAL_EVENT()
    allow_first_set = _REAL_EVENT()
    completed = _FirstSetPausesEvent(set_entered, allow_first_set)
    waiter = AdmissionWaiter(1, 99, time.monotonic() + 1, False, completed=completed)
    with broker._condition:
        broker._waiters.append(waiter)
        broker._waiter_snapshot = (waiter,)
    releaser = threading.Thread(target=lambda: broker.release(active))
    releaser.start()
    assert set_entered.wait(timeout=2)

    try:
        broker.abandon()
        assert completed.is_set()
        assert waiter.state is AdmissionState.CLOSED
    finally:
        allow_first_set.set()
        releaser.join(timeout=2)

    assert not releaser.is_alive()


@pytest.mark.parametrize("paused_index", (0, 1))
def test_two_actor_fatals_each_complete_epoch_fanout(paused_index: int) -> None:
    first_set_entered = _REAL_EVENT()
    allow_first_set = _REAL_EVENT()
    retire = _FirstSetPausesEvent(first_set_entered, allow_first_set)
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
    paused = threading.Thread(
        target=lambda: handles[paused_index].publish(
            runtimes[paused_index], errors[paused_index]
        )
    )
    paused.start()
    assert first_set_entered.wait(timeout=2)
    other = 1 - paused_index
    handles[other].publish(runtimes[other], errors[other])

    assert paused.is_alive()
    assert broker.snapshot().closed and push.snapshot().closed
    assert all(runtime.stop_requested for runtime in runtimes)
    allow_first_set.set()
    paused.join(timeout=2)

    assert not paused.is_alive()
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


def test_push_reads_fatal_reason_during_retirement_fanout_window() -> None:
    retire_entered = _REAL_EVENT()
    allow_fanout = _REAL_EVENT()
    retire = _SetThenPauseEvent(retire_entered, allow_fanout)
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    push = PushBuffer(1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push, retire_event=retire)
    handle = pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
    runtime = ActorRuntime(1, ())
    error = ConnectionClosedError("fatal before push fanout")
    runtime.fatal_error = error
    publisher = threading.Thread(target=lambda: handle.publish(runtime, error))
    publisher.start()
    assert retire_entered.wait(timeout=2)

    try:
        with pytest.raises(ConnectionClosedError) as polled:
            push.poll()
        with pytest.raises(ConnectionClosedError) as drained:
            push.drain()
        assert polled.value is error
        assert drained.value is error
    finally:
        allow_fanout.set()
        publisher.join(timeout=2)

    assert not publisher.is_alive()


def test_two_actor_fatal_reason_is_sticky_when_publication_cell_is_contended() -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    push = PushBuffer(1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push, retire_event=retire)
    runtimes = (ActorRuntime(1, ()), ActorRuntime(1, ()))
    for runtime in runtimes:
        assert guard.add_runtime(runtime, pool_epoch=1, broker=broker)
    handles = tuple(
        pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
        for _ in runtimes
    )
    errors = (
        ConnectionClosedError("registered-first fatal"),
        ConnectionClosedError("published-first fatal"),
    )

    with guard._failure_cell.lock:
        runtimes[1].fatal_error = errors[1]
        handles[1].publish(runtimes[1], errors[1])
        chosen = guard.failure()
        assert chosen is errors[1]

        runtimes[0].fatal_error = errors[0]
        handles[0].publish(runtimes[0], errors[0])
        assert guard.failure() is chosen

        with pytest.raises(ConnectionClosedError) as polled:
            push.poll()
        with pytest.raises(ConnectionClosedError) as drained:
            push.drain()
        assert polled.value is chosen
        assert drained.value is chosen

    assert guard.finish_epoch(pool_epoch=1, broker=broker) is chosen
    assert guard.failure() is chosen


def test_two_actor_fatal_cannot_overwrite_push_reason_after_guard_choice() -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    push = _PauseFirstFatalPublishBuffer(1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push, retire_event=retire)
    runtimes = (ActorRuntime(1, ()), ActorRuntime(1, ()))
    handles = tuple(
        pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
        for _ in runtimes
    )
    errors = (
        ConnectionClosedError("paused first fatal"),
        ConnectionClosedError("interleaved second fatal"),
    )
    for runtime, error in zip(runtimes, errors):
        runtime.fatal_error = error
        assert guard.add_runtime(runtime, pool_epoch=1, broker=broker)

    publisher = threading.Thread(
        name="first-fatal-publisher",
        target=lambda: handles[0].publish(runtimes[0], errors[0]),
    )
    publisher.start()
    assert push.read_entered.wait(timeout=2)
    handles[1].publish(runtimes[1], errors[1])
    chosen = guard.failure()

    push.allow_read.set()
    publisher.join(timeout=2)
    assert not publisher.is_alive()

    with pytest.raises(ConnectionClosedError) as polled:
        push.poll()
    with pytest.raises(ConnectionClosedError) as drained:
        push.drain()
    assert polled.value is chosen
    assert drained.value is chosen
    assert guard.finish_epoch(pool_epoch=1, broker=broker) is chosen
    assert guard.failure() is chosen


def test_delayed_old_epoch_reason_cannot_poison_new_epoch_resolver() -> None:
    guard = PoolRuntimeGuard()
    old_retire = _REAL_EVENT()
    old_broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    old_push = PushBuffer(1)
    guard.configure(old_broker, old_push, retire_event=old_retire)
    old_handle = pool_module.ActorFatalHandle(
        weakref.ref(guard), 1, weakref.ref(old_broker), old_retire
    )
    old_runtime = ActorRuntime(1, ())

    new_retire = _REAL_EVENT()
    new_broker = LeaseBroker(2, pool_size=1, max_pending_requests=1)
    new_push = PushBuffer(2)
    guard.configure(new_broker, new_push, retire_event=new_retire)
    new_handle = pool_module.ActorFatalHandle(
        weakref.ref(guard), 2, weakref.ref(new_broker), new_retire
    )
    new_runtime = ActorRuntime(2, ())

    old_error = ConnectionClosedError("delayed old epoch fatal")
    old_runtime.fatal_error = old_error
    old_handle.publish(old_runtime, old_error)
    with pytest.raises(ConnectionClosedError) as old_polled:
        old_push.poll()
    assert old_polled.value is old_error
    assert guard.failure() is None
    assert new_push.poll() is None
    assert not new_retire.is_set()

    new_error = ConnectionClosedError("new epoch fatal")
    new_runtime.fatal_error = new_error
    new_handle.publish(new_runtime, new_error)
    with pytest.raises(ConnectionClosedError) as polled:
        new_push.poll()
    assert polled.value is new_error
    assert guard.failure() is new_error
    assert guard.finish_epoch(pool_epoch=2, broker=new_broker) is new_error
    assert guard.finish_epoch(pool_epoch=1, broker=old_broker) is None


@pytest.mark.parametrize("owner", ("pending_count", "snapshot", "poll", "drain"))
def test_push_owner_lazily_drains_after_abandon_condition_contention(owner: str) -> None:
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
    error = ConnectionClosedError(f"fatal deferred drain via {owner}")
    push.abandon(error)
    release.set()
    holder.join(timeout=2)
    assert not holder.is_alive()

    if owner == "pending_count":
        assert push.pending_count == 0
    elif owner == "snapshot":
        assert push.snapshot().closed
    else:
        with pytest.raises(ConnectionClosedError) as raised:
            push.poll() if owner == "poll" else push.drain()
        assert raised.value is error

    snapshot = push.snapshot()
    assert snapshot.closed
    assert snapshot.frame_count == snapshot.byte_count == 0
    assert push.pending_count == 0


def test_graceful_close_publication_preserves_buffered_consumption() -> None:
    push = PushBuffer(1)
    frames = (_frame(1, 1), _frame(1, 2))
    assert all(push.offer_nowait(frame) for frame in frames)

    push.publish_close(None)

    assert push.poll() is frames[0]
    assert push.drain() == [frames[1]]
    assert push.poll() is None
    assert push.drain() == []


def test_pinned_push_pollers_preserve_epoch_fatal_identity() -> None:
    retire = _REAL_EVENT()
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1, retire_event=retire)
    push = PushBuffer(1, retire_event=retire)
    lease = broker.acquire(time.monotonic() + 1, pinned=True)
    proxy = PinnedTransportProxy(broker, lease, object(), push, timeout=1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push, retire_event=retire)
    handle = pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
    runtime = ActorRuntime(1, ())
    error = ConnectionClosedError("pinned epoch fatal")
    runtime.fatal_error = error

    handle.publish(runtime, error)

    with pytest.raises(ConnectionClosedError) as polled:
        proxy.poll_push()
    with pytest.raises(ConnectionClosedError) as drained:
        proxy.drain_pushes()
    assert polled.value is error
    assert drained.value is error
