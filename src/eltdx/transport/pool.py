"""Bounded FIFO connection pool for the 7709 Actor transport."""

from __future__ import annotations

import sys
import threading
import time
import weakref
from collections import deque
from collections.abc import Sequence
from concurrent.futures import ALL_COMPLETED, FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Iterator

from eltdx.exceptions import (
    ConnectionClosedError,
    PoolBusyError,
    ResponseTimeoutError,
    TransportCloseTimeoutError,
)
from eltdx.hosts import DEFAULT_HOSTS, DEFAULT_PROBE_TIMEOUT, DEFAULT_PROBE_WORKERS, resolve_hosts, sort_hosts_by_latency, unique_hosts

from .push import PushBuffer
from .actor import ActorRuntime, ActorSnapshot, abandon_actor, actor_snapshot, request_actor_stop
from .socket import (
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_PUSH_QUEUE_BYTES,
    DEFAULT_PUSH_QUEUE_SIZE,
    SocketTransport,
    _acquire_gate_token,
    _parse_push,
    _release_gate_token,
    _requires_dns,
)

DEFAULT_MAX_PENDING_REQUESTS = 256
_INTERNAL_EVENT = threading.Event
_CANCELLED_LEASE = _INTERNAL_EVENT()
_CANCELLED_LEASE.set()


class PoolState(Enum):
    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    CLOSING = auto()
    FAILED = auto()
    FAILED_CLOSING = auto()
    FAILED_CLOSED = auto()


class AdmissionState(Enum):
    WAITING = auto()
    ASSIGNED = auto()
    TIMED_OUT = auto()
    REJECTED = auto()
    CLOSED = auto()


class LeaseState(Enum):
    ACTIVE = auto()
    RELEASED = auto()


class PinState(Enum):
    OPEN = auto()
    CLOSING = auto()
    FAILED = auto()
    CLOSED = auto()


class GuardState(Enum):
    INACTIVE = auto()
    ACTIVE = auto()
    SEALED = auto()


@dataclass(slots=True)
class SlotLease:
    pool_epoch: int
    lease_id: int
    slot_id: int
    pinned: bool
    cancellation: threading.Event | None = None
    state: LeaseState = LeaseState.ACTIVE


def _mark_lease_cancelled(lease: SlotLease) -> None:
    cancellation = lease.cancellation
    if cancellation is None:
        lease.cancellation = _CANCELLED_LEASE
    else:
        cancellation.set()


@dataclass(slots=True)
class AdmissionWaiter:
    pool_epoch: int
    waiter_id: int
    deadline: float
    pinned: bool
    slot_count: int = 1
    state: AdmissionState = AdmissionState.WAITING
    assigned_lease: SlotLease | None = None
    assigned_leases: tuple[SlotLease, ...] = ()
    error: BaseException | None = None
    completed: threading.Event = field(default_factory=threading.Event)
    cancelled: threading.Event = field(default_factory=_INTERNAL_EVENT)
    wake_started: bool = False


def _wake_admission_waiter(waiter: AdmissionWaiter) -> None:
    waiter.wake_started = True
    waiter.completed.set()


@dataclass(frozen=True, slots=True)
class BrokerSnapshot:
    pool_epoch: int
    idle_slots: int
    waiter_count: int
    pin_waiter_count: int
    active_leases: int
    closed: bool


@dataclass(slots=True, eq=False)
class _GuardFailurePublication:
    pool_epoch: int | None
    broker: LeaseBroker | None
    failure: tuple[ActorRuntime, BaseException] | None = None
    cleanup_error: BaseException | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)


class LeaseBroker:
    """Pool scheduling core with no references to facades or Actor runtimes."""

    def __init__(
        self,
        pool_epoch: int,
        pool_size: int,
        max_pending_requests: int,
        *,
        retire_event: threading.Event | None = None,
    ) -> None:
        self.pool_epoch = pool_epoch
        self.pool_size = pool_size
        self.max_pending_requests = max_pending_requests
        self._condition = threading.Condition()
        self._idle_slots = deque(range(pool_size))
        self._waiters: deque[AdmissionWaiter] = deque()
        self._active_leases: dict[int, SlotLease] = {}
        self._active_lease_snapshot: tuple[SlotLease, ...] = ()
        self._waiter_snapshot: tuple[AdmissionWaiter, ...] = ()
        self._assigned_waiters: dict[int, AdmissionWaiter] = {}
        self._lease_release_published = _INTERNAL_EVENT()
        self._waiter_counter = 0
        self._lease_counter = 0
        self._pin_waiters = 0
        self._pin_waiter_events: dict[Any, threading.Event | None] = {}
        self._pin_waiter_snapshot: tuple[Any, ...] = ()
        self._retire_event = retire_event or _INTERNAL_EVENT()
        self._close_requested = _INTERNAL_EVENT()
        self._closed = False
        self._drained = False

    @property
    def retired(self) -> bool:
        return self._termination_requested()

    def bind_retire_event(self, retire_event: threading.Event) -> None:
        previous = self._retire_event
        self._retire_event = retire_event
        if previous is not retire_event and previous.is_set():
            retire_event.set()

    def _termination_requested(self) -> bool:
        return self._retire_event.is_set() or self._close_requested.is_set()

    def _refresh_waiter_snapshot_locked(self) -> None:
        self._waiter_snapshot = tuple(self._waiters) + tuple(self._assigned_waiters.values())

    def _drain_retired_locked(self, *, force_wake: bool = True) -> None:
        waiters = self._waiter_snapshot
        if self._waiters or self._assigned_waiters:
            waiters = tuple(self._waiters) + tuple(self._assigned_waiters.values())
        for waiter in waiters:
            if waiter.state in (AdmissionState.WAITING, AdmissionState.ASSIGNED):
                waiter.state = AdmissionState.CLOSED
                waiter.error = ConnectionClosedError("7709 pool closed during admission")
            waiter.cancelled.set()
            if force_wake or not waiter.wake_started:
                _wake_admission_waiter(waiter)
        self._waiters.clear()
        self._assigned_waiters.clear()
        self._waiter_snapshot = ()
        self._lease_release_published.clear()
        for lease in self._active_leases.values():
            _mark_lease_cancelled(lease)
            lease.state = LeaseState.RELEASED
        self._active_leases.clear()
        self._active_lease_snapshot = ()
        self._idle_slots.clear()
        pin_wake = self._pin_waiter_snapshot
        self._pin_waiter_events.clear()
        self._pin_waiter_snapshot = ()
        self._pin_waiters = 0
        for completed in pin_wake:
            completed.set()
        self._closed = True
        self._drained = True

    def _publish_close_request(self, *, force_wake: bool = True) -> None:
        self._close_requested.set()
        for waiter in self._waiter_snapshot:
            waiter.cancelled.set()
            if force_wake or not waiter.wake_started:
                _wake_admission_waiter(waiter)
        for lease in self._active_lease_snapshot:
            _mark_lease_cancelled(lease)
        for completed in self._pin_waiter_snapshot:
            completed.set()

    def acquire(self, deadline: float, *, pinned: bool = False) -> SlotLease:
        return self._acquire_slots(1, deadline, pinned=pinned)[0]

    def acquire_many(self, count: int, deadline: float) -> tuple[SlotLease, ...]:
        if count <= 0 or count > self.pool_size:
            raise ValueError(f"slot count must be between 1 and {self.pool_size}")
        return self._acquire_slots(count, deadline, pinned=False)

    def _acquire_slots(self, count: int, deadline: float, *, pinned: bool) -> tuple[SlotLease, ...]:
        initial_wake: list[AdmissionWaiter] = []
        final_wake: list[AdmissionWaiter] = []
        acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out during queue")
        try:
            self._assign_waiters_locked(initial_wake)
            if self._termination_requested() or self._closed:
                self._drain_retired_locked()
                raise ConnectionClosedError("7709 pool is closed")
            if time.monotonic() >= deadline:
                raise ResponseTimeoutError("7709 response timed out during queue")
            if len(self._idle_slots) >= count and not self._waiters:
                leases = tuple(
                    self._new_lease_locked(self._idle_slots.popleft(), pinned)
                    for _ in range(count)
                )
                if self._termination_requested():
                    self._drain_retired_locked()
                    raise ConnectionClosedError("7709 pool closed during admission")
                return leases
            if len(self._waiters) + self._pin_waiters >= self.max_pending_requests:
                raise PoolBusyError("7709 pool admission queue is full")
            self._waiter_counter += 1
            completed = threading.Event()
            waiter = AdmissionWaiter(
                self.pool_epoch,
                self._waiter_counter,
                deadline,
                pinned,
                slot_count=count,
                completed=completed,
            )
            self._waiters.append(waiter)
            self._refresh_waiter_snapshot_locked()
            self._assign_waiters_locked(initial_wake)
            if self._termination_requested():
                self._drain_retired_locked()
                raise ConnectionClosedError("7709 pool closed during admission")
            self._condition.notify_all()
        finally:
            self._condition.release()
            for assigned in initial_wake:
                _wake_admission_waiter(assigned)
        while True:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                woke = waiter.completed.wait(remaining)
            except BaseException:
                waiter.cancelled.set()
                wake: list[AdmissionWaiter] = []
                acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
                if acquired:
                    try:
                        if waiter.state is AdmissionState.WAITING:
                            try:
                                self._waiters.remove(waiter)
                            except ValueError:
                                pass
                            waiter.state = AdmissionState.REJECTED
                        elif waiter.state is AdmissionState.ASSIGNED:
                            waiter.state = AdmissionState.REJECTED
                            self._assigned_waiters.pop(waiter.waiter_id, None)
                        self._refresh_waiter_snapshot_locked()
                        self._assign_waiters_locked(wake)
                    finally:
                        self._condition.release()
                for assigned in wake:
                    _wake_admission_waiter(assigned)
                self._wake_next_live_waiter()
                raise
            if not woke:
                break
            recheck_wake: list[AdmissionWaiter] = []
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if not acquired:
                waiter.cancelled.set()
                self._wake_next_live_waiter()
                raise ResponseTimeoutError("7709 response timed out during queue")
            try:
                self._assign_waiters_locked(recheck_wake)
                terminal = waiter.state is not AdmissionState.WAITING
                if not terminal:
                    waiter.wake_started = False
                    waiter.completed.clear()
                    self._assign_waiters_locked(recheck_wake)
                    terminal = waiter.state is not AdmissionState.WAITING
            finally:
                self._condition.release()
            for assigned in recheck_wake:
                _wake_admission_waiter(assigned)
            if terminal:
                break
        if not woke:
            wake: list[AdmissionWaiter] = []
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if acquired:
                try:
                    if waiter.state is AdmissionState.WAITING:
                        waiter.cancelled.set()
                        try:
                            self._waiters.remove(waiter)
                        except ValueError:
                            pass
                        waiter.state = AdmissionState.TIMED_OUT
                        waiter.error = ResponseTimeoutError("7709 response timed out during queue")
                        wake.append(waiter)
                        self._refresh_waiter_snapshot_locked()
                        self._assign_waiters_locked(wake)
                finally:
                    self._condition.release()
            for assigned in wake:
                _wake_admission_waiter(assigned)
        acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            waiter.cancelled.set()
            self._wake_next_live_waiter()
            raise ResponseTimeoutError("7709 response timed out during queue")
        try:
            self._assign_waiters_locked(final_wake)
            if waiter.state is AdmissionState.ASSIGNED and len(waiter.assigned_leases) == count:
                leases = waiter.assigned_leases
                valid = (
                    not self._termination_requested()
                    and not self._closed
                    and not waiter.cancelled.is_set()
                    and time.monotonic() < deadline
                    and all(
                        lease.state is LeaseState.ACTIVE
                        and (lease.cancellation is None or not lease.cancellation.is_set())
                        and self._active_leases.get(lease.lease_id) is lease
                        for lease in leases
                    )
                )
                if valid:
                    self._assigned_waiters.pop(waiter.waiter_id, None)
                    self._refresh_waiter_snapshot_locked()
                    if self._termination_requested():
                        self._drain_retired_locked()
                        raise ConnectionClosedError("7709 pool closed during admission")
                    return leases
                if waiter.cancelled.is_set() or time.monotonic() >= deadline:
                    waiter.cancelled.set()
                    self._assign_waiters_locked(final_wake)
                    waiter.state = AdmissionState.TIMED_OUT
                    waiter.error = ResponseTimeoutError("7709 response timed out during queue")
                else:
                    waiter.state = AdmissionState.CLOSED
                    waiter.error = ConnectionClosedError("7709 pool closed during admission")
            self._assigned_waiters.pop(waiter.waiter_id, None)
            self._refresh_waiter_snapshot_locked()
            error = waiter.error
        finally:
            self._condition.release()
            for assigned in final_wake:
                _wake_admission_waiter(assigned)
        if error is not None:
            raise error
        raise ResponseTimeoutError("7709 response timed out during queue")

    def release(self, lease: SlotLease, *, deadline: float | None = None) -> bool:
        wake: list[AdmissionWaiter] = []
        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool broker release blocked before deadline")
        try:
            current = self._active_leases.get(lease.lease_id)
            if self._termination_requested():
                owned = current is lease and lease.state is not LeaseState.RELEASED
                self._drain_retired_locked()
                return owned
            if current is not lease or lease.state is LeaseState.RELEASED:
                self._assign_waiters_locked(wake)
                return False
            lease.state = LeaseState.RELEASED
            del self._active_leases[lease.lease_id]
            self._active_lease_snapshot = tuple(self._active_leases.values())
            if self._termination_requested() or self._closed:
                self._drain_retired_locked()
                return True
            self._idle_slots.append(lease.slot_id)
            if self._termination_requested():
                self._drain_retired_locked()
                return True
            self._assign_waiters_locked(wake)
        finally:
            self._condition.release()
            for waiter in wake:
                _wake_admission_waiter(waiter)
        return True

    def _assign_waiters_locked(self, wake: list[AdmissionWaiter]) -> None:
        while True:
            self._lease_release_published.clear()
            if self._termination_requested():
                self._drain_retired_locked()
                return
            self._assign_waiters_pass_locked(wake)
            if self._termination_requested():
                self._drain_retired_locked()
                return
            if not self._lease_release_published.is_set():
                return

    def _assign_waiters_pass_locked(self, wake: list[AdmissionWaiter]) -> None:
        if self._termination_requested():
            self._drain_retired_locked()
            return
        self._reclaim_cancelled_leases_locked()
        self._reclaim_cancelled_pin_waiters_locked()
        while self._waiters:
            waiter = self._waiters[0]
            if waiter.state is not AdmissionState.WAITING or waiter.pool_epoch != self.pool_epoch:
                self._waiters.popleft()
                continue
            if waiter.cancelled.is_set() or time.monotonic() >= waiter.deadline:
                self._waiters.popleft()
                waiter.state = AdmissionState.TIMED_OUT
                waiter.error = ResponseTimeoutError("7709 response timed out during queue")
                wake.append(waiter)
                continue
            if len(self._idle_slots) < waiter.slot_count:
                self._refresh_waiter_snapshot_locked()
                return
            self._waiters.popleft()
            leases = tuple(
                self._new_lease_locked(
                    self._idle_slots.popleft(),
                    waiter.pinned,
                    cancellation=waiter.cancelled,
                )
                for _ in range(waiter.slot_count)
            )
            waiter.assigned_leases = leases
            waiter.assigned_lease = leases[0]
            waiter.state = AdmissionState.ASSIGNED
            self._assigned_waiters[waiter.waiter_id] = waiter
            self._refresh_waiter_snapshot_locked()
            if self._termination_requested():
                self._drain_retired_locked()
                return
            wake.append(waiter)
        self._refresh_waiter_snapshot_locked()

    def validate(self, lease: SlotLease, *, deadline: float | None = None) -> bool:
        wake: list[AdmissionWaiter] = []
        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out validating pool lease")
        try:
            self._assign_waiters_locked(wake)
            valid = (
                not self._termination_requested()
                and not self._closed
                and lease.pool_epoch == self.pool_epoch
                and lease.state is LeaseState.ACTIVE
                and (lease.cancellation is None or not lease.cancellation.is_set())
                and self._active_leases.get(lease.lease_id) is lease
            )
            if self._termination_requested():
                self._drain_retired_locked()
                return False
            return valid
        finally:
            self._condition.release()
            for assigned in wake:
                _wake_admission_waiter(assigned)

    def reserve_pin_waiter(
        self,
        completed: Any = None,
        *,
        cancelled: threading.Event | None = None,
        deadline: float | None = None,
    ) -> None:
        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out reserving pin admission")
        try:
            self._reclaim_cancelled_pin_waiters_locked()
            if self._termination_requested() or self._closed:
                self._drain_retired_locked()
                raise ConnectionClosedError("7709 pool is closed")
            if len(self._waiters) + self._pin_waiters >= self.max_pending_requests:
                raise PoolBusyError("7709 pool admission queue is full")
            self._pin_waiters += 1
            if completed is not None:
                self._pin_waiter_events[completed] = cancelled
                self._pin_waiter_snapshot = tuple(self._pin_waiter_events)
            if self._termination_requested():
                self._drain_retired_locked()
                raise ConnectionClosedError("7709 pool closed during pin admission")
            self._condition.notify_all()
        finally:
            self._condition.release()

    def release_pin_waiter(self, completed: Any = None, *, deadline: float | None = None) -> None:
        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool pin reservation release blocked before deadline")
        try:
            if self._termination_requested():
                self._drain_retired_locked()
                return
            self._reclaim_cancelled_pin_waiters_locked()
            if completed is not None:
                if completed not in self._pin_waiter_events:
                    if self._termination_requested():
                        self._drain_retired_locked()
                    return
                del self._pin_waiter_events[completed]
                self._pin_waiter_snapshot = tuple(self._pin_waiter_events)
            if self._pin_waiters > 0:
                self._pin_waiters -= 1
            if self._termination_requested():
                self._drain_retired_locked()
        finally:
            self._condition.release()

    def close(self, *, deadline: float | None = None) -> None:
        self._publish_close_request(force_wake=False)
        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool broker close blocked before deadline")
        try:
            self._drain_retired_locked(force_wake=False)
            self._condition.notify_all()
        finally:
            self._condition.release()

    def abandon(self) -> None:
        self._publish_close_request()
        if not self._condition.acquire(blocking=False):
            return
        try:
            self._drain_retired_locked()
            self._condition.notify_all()
        finally:
            self._condition.release()

    def publish_lease_release(self, lease: SlotLease) -> None:
        _mark_lease_cancelled(lease)
        self._lease_release_published.set()
        if self._termination_requested():
            for waiter in self._waiter_snapshot:
                if not waiter.wake_started:
                    _wake_admission_waiter(waiter)
            for completed in self._pin_waiter_snapshot:
                completed.set()
            return
        self._wake_next_live_waiter()

    def _wake_next_live_waiter(self) -> None:
        if self._termination_requested() or self._closed:
            for waiter in self._waiter_snapshot:
                if not waiter.wake_started:
                    _wake_admission_waiter(waiter)
            return
        now = time.monotonic()
        for waiter in self._waiter_snapshot:
            if (
                waiter.state is AdmissionState.WAITING
                and waiter.pool_epoch == self.pool_epoch
                and not waiter.cancelled.is_set()
                and now < waiter.deadline
            ):
                _wake_admission_waiter(waiter)
                return

    def snapshot(self) -> BrokerSnapshot:
        wake: list[AdmissionWaiter] = []
        with self._condition:
            if self._termination_requested():
                self._drain_retired_locked()
            self._assign_waiters_locked(wake)
            self._reclaim_cancelled_pin_waiters_locked()
            if self._termination_requested():
                self._drain_retired_locked()
            snapshot = BrokerSnapshot(
                pool_epoch=self.pool_epoch,
                idle_slots=len(self._idle_slots),
                waiter_count=sum(waiter.state is AdmissionState.WAITING for waiter in self._waiters),
                pin_waiter_count=self._pin_waiters,
                active_leases=len(self._active_leases),
                closed=self._closed,
            )
        for assigned in wake:
            _wake_admission_waiter(assigned)
        return snapshot

    def allows_heartbeat(self, *, blocking: bool = True) -> bool:
        if self._termination_requested():
            return False
        if not self._condition.acquire(blocking=blocking):
            return False
        try:
            if self._termination_requested():
                self._drain_retired_locked()
                return False
            self._reclaim_cancelled_pin_waiters_locked()
            if self._closed or self._pin_waiters:
                return False
            if any(waiter.state is AdmissionState.WAITING for waiter in self._waiters):
                return False
            allowed = not any(
                lease.state is LeaseState.ACTIVE and not lease.pinned
                for lease in self._active_leases.values()
            )
            if self._termination_requested():
                self._drain_retired_locked()
                return False
            return allowed
        finally:
            self._condition.release()

    def wait_for_waiters(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(
                lambda: sum(waiter.state is AdmissionState.WAITING for waiter in self._waiters) >= count,
                timeout=timeout,
            )

    def wait_for_pin_waiters(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            self._reclaim_cancelled_pin_waiters_locked()
            return self._condition.wait_for(lambda: self._pin_waiters >= count, timeout=timeout)

    def _reclaim_cancelled_leases_locked(self) -> None:
        if self._termination_requested():
            self._drain_retired_locked()
            return
        for lease_id, lease in tuple(self._active_leases.items()):
            if lease.cancellation is None or not lease.cancellation.is_set():
                continue
            lease.state = LeaseState.RELEASED
            del self._active_leases[lease_id]
            self._active_lease_snapshot = tuple(self._active_leases.values())
            if not self._termination_requested() and not self._closed:
                self._idle_slots.append(lease.slot_id)
            if self._termination_requested():
                self._drain_retired_locked()
                return

    def _reclaim_cancelled_pin_waiters_locked(self) -> None:
        if self._termination_requested():
            self._drain_retired_locked()
            return
        for completed, cancelled in tuple(self._pin_waiter_events.items()):
            if cancelled is None or not cancelled.is_set():
                continue
            del self._pin_waiter_events[completed]
            self._pin_waiter_snapshot = tuple(self._pin_waiter_events)
            if self._pin_waiters > 0:
                self._pin_waiters -= 1
            if self._termination_requested():
                self._drain_retired_locked()
                return
        if self._termination_requested():
            self._drain_retired_locked()

    def _new_lease_locked(
        self,
        slot_id: int,
        pinned: bool,
        *,
        cancellation: threading.Event | None = None,
    ) -> SlotLease:
        self._lease_counter += 1
        lease = SlotLease(
            self.pool_epoch,
            self._lease_counter,
            slot_id,
            pinned,
            cancellation,
        )
        self._active_leases[lease.lease_id] = lease
        self._active_lease_snapshot = tuple(self._active_leases.values())
        return lease


class LeaseCompletion:
    propagate_settlement_error = True

    def __init__(self, broker: LeaseBroker, lease: SlotLease, deadline: float | None = None) -> None:
        self._broker_ref = weakref.ref(broker)
        self._lease = lease
        self._deadline = deadline
        self._settle_lock = threading.Lock()
        self._publish_lock = threading.Lock()
        self._released = False
        self._published = threading.Event()

    def publish(self, ticket: object | None) -> None:
        if self._published.is_set() or not self._publish_lock.acquire(blocking=False):
            return
        try:
            if self._published.is_set():
                return
            broker = self._broker_ref()
            if broker is None:
                _mark_lease_cancelled(self._lease)
            else:
                broker.publish_lease_release(self._lease)
            self._published.set()
        finally:
            self._publish_lock.release()

    publish_nonblocking = publish

    def settle(self, ticket: object | None = None) -> None:
        with self._settle_lock:
            if self._released:
                return
            self._released = True
        broker = self._broker_ref()
        if broker is not None:
            try:
                broker.release(self._lease, deadline=self._deadline)
            except BaseException as exc:
                _mark_lease_cancelled(self._lease)
                if not isinstance(exc, TransportCloseTimeoutError):
                    raise

    def __call__(self, ticket: object | None) -> None:
        self.publish(ticket)
        self.settle(ticket)


class HeartbeatAdmissionGuard:
    """Allow pooled heartbeats without retaining the pool scheduling core."""

    def __init__(self, broker: LeaseBroker) -> None:
        self._broker_ref = weakref.ref(broker)

    def __call__(self) -> bool:
        return self.try_allowed()

    def try_allowed(self) -> bool:
        broker = self._broker_ref()
        return broker is not None and broker.allows_heartbeat(blocking=False)


class PoolRuntimeGuard:
    """Facade-independent runtime group used by fatal paths and finalization."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._retire_requested = threading.Event()
        self._broker: LeaseBroker | None = None
        self._push_buffer: PushBuffer | None = None
        self._runtimes: list[ActorRuntime] = []
        self._runtime_snapshot: tuple[ActorRuntime, ...] = ()
        self._fatal_handle_snapshot: tuple[ActorFatalHandle, ...] = ()
        self._fatal_error: BaseException | None = None
        self._epoch: int | None = None
        self._state = GuardState.INACTIVE
        self._failure_cell = _GuardFailurePublication(None, None)
        self._publication_snapshot: tuple[
            GuardState,
            int | None,
            LeaseBroker | None,
            threading.Event,
            PushBuffer | None,
            tuple[ActorRuntime, ...],
            _GuardFailurePublication,
        ] = (
            self._state,
            self._epoch,
            self._broker,
            self._retire_requested,
            self._push_buffer,
            (),
            self._failure_cell,
        )

    def configure(
        self,
        broker: LeaseBroker,
        push_buffer: PushBuffer,
        *,
        retire_event: threading.Event | None = None,
        deadline: float | None = None,
    ) -> None:
        if deadline is None:
            self._lock.acquire()
            acquired = True
        else:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out configuring pool runtime")
        try:
            self._retire_requested = retire_event or threading.Event()
            broker.bind_retire_event(self._retire_requested)
            push_buffer.bind_retire_event(self._retire_requested)
            self._broker = broker
            self._push_buffer = push_buffer
            self._runtimes = []
            self._runtime_snapshot = ()
            self._fatal_handle_snapshot = ()
            self._fatal_error = None
            self._epoch = broker.pool_epoch
            self._state = GuardState.ACTIVE
            self._failure_cell = _GuardFailurePublication(broker.pool_epoch, broker)
            self._refresh_publication_snapshot_locked()
        finally:
            self._lock.release()

    def is_active(
        self,
        *,
        pool_epoch: int,
        broker: LeaseBroker | None,
        deadline: float | None = None,
    ) -> bool:
        if self._retire_requested.is_set():
            return False
        if deadline is None:
            self._lock.acquire()
            acquired = True
        else:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out checking pool runtime")
        try:
            return (
                self._state is GuardState.ACTIVE
                and not self._retire_requested.is_set()
                and self._fatal_error is None
                and broker is not None
                and broker is self._broker
                and pool_epoch == self._epoch
            )
        finally:
            self._lock.release()

    def add_runtime(
        self,
        runtime: ActorRuntime,
        *,
        pool_epoch: int,
        broker: LeaseBroker | None = None,
        deadline: float | None = None,
    ) -> bool:
        if deadline is None:
            self._lock.acquire()
            acquired = True
        else:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            error = ResponseTimeoutError("7709 response timed out registering pool runtime")
            error._eltdx_deadline = deadline  # type: ignore[attr-defined]
            raise error
        try:
            accepted = (
                self._state is GuardState.ACTIVE
                and not self._retire_requested.is_set()
                and self._fatal_error is None
                and self._broker is not None
                and runtime.runtime_epoch == self._epoch
                and pool_epoch == self._epoch
                and broker is self._broker
            )
            if accepted and all(item is not runtime for item in self._runtimes):
                self._runtimes.append(runtime)
                self._runtime_snapshot = tuple(self._runtimes)
                self._refresh_publication_snapshot_locked()
            if accepted and self._retire_requested.is_set():
                accepted = False
        finally:
            self._lock.release()
        if not accepted:
            if deadline is None:
                request_actor_stop(runtime)
            else:
                request_actor_stop(runtime, deadline=deadline)
        return accepted

    def fail(
        self,
        runtime: ActorRuntime,
        error: BaseException,
        *,
        pool_epoch: int,
        broker: LeaseBroker | None = None,
        deadline: float | None = None,
    ) -> None:
        if deadline is None:
            self._lock.acquire()
            acquired = True
        else:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            request_actor_stop(runtime, deadline=deadline)
            raise ResponseTimeoutError("7709 response timed out publishing pool runtime failure")
        try:
            accepted = (
                self._state in (GuardState.ACTIVE, GuardState.SEALED)
                and self._broker is not None
                and runtime.runtime_epoch == self._epoch
                and pool_epoch == self._epoch
                and broker is self._broker
            )
            if accepted:
                cleanup_cell = self._failure_cell
                if self._fatal_error is None:
                    self._fatal_error = error
                if all(item is not runtime for item in self._runtimes):
                    self._runtimes.append(runtime)
                current_broker = self._broker
                push_buffer = self._push_buffer
                runtimes = tuple(self._runtimes)
            else:
                cleanup_cell = None
                current_broker = None
                push_buffer = None
                runtimes = (runtime,)
        finally:
            self._lock.release()
        cleanup_errors: list[BaseException] = []
        if current_broker is not None:
            try:
                current_broker.close(deadline=deadline)
            except BaseException as exc:
                cleanup_errors.append(exc)
        if push_buffer is not None:
            try:
                if deadline is None:
                    push_buffer.close(error)
                else:
                    push_buffer.close_before_deadline(deadline, error)
            except BaseException as exc:
                cleanup_errors.append(exc)
        for item in runtimes:
            try:
                if deadline is None:
                    request_actor_stop(item)
                else:
                    request_actor_stop(item, deadline=deadline)
            except BaseException as exc:
                cleanup_errors.append(exc)
        if cleanup_cell is not None:
            if deadline is None:
                cleanup_cell.lock.acquire()
                cleanup_acquired = True
            else:
                cleanup_acquired = cleanup_cell.lock.acquire(
                    timeout=max(0.0, deadline - time.monotonic())
                )
            if not cleanup_acquired:
                raise TransportCloseTimeoutError(
                    "7709 pool fatal cleanup publication blocked before deadline"
                )
            try:
                if (
                    cleanup_cell.pool_epoch == pool_epoch
                    and cleanup_cell.broker is broker
                ):
                    if cleanup_errors and cleanup_cell.cleanup_error is None:
                        cleanup_cell.cleanup_error = cleanup_errors[0]
                    elif not cleanup_errors:
                        cleanup_cell.cleanup_error = None
            finally:
                cleanup_cell.lock.release()
        unexpected_cleanup = next(
            (
                item
                for item in cleanup_errors
                if not isinstance(item, (TransportCloseTimeoutError, ResponseTimeoutError))
            ),
            None,
        )
        if unexpected_cleanup is not None:
            raise unexpected_cleanup

    def publish_failure(
        self,
        runtime: ActorRuntime,
        error: BaseException,
        *,
        pool_epoch: int,
        broker: LeaseBroker | None,
    ) -> None:
        state, epoch, current_broker, retire_event, push_buffer, runtimes, failure_cell = (
            self._publication_snapshot
        )
        accepted = (
            state in (GuardState.ACTIVE, GuardState.SEALED)
            and epoch == pool_epoch
            and broker is not None
            and current_broker is broker
        )
        if not accepted:
            abandon_actor(runtime)
            return
        retire_event.set()
        broker.abandon()
        if push_buffer is not None:
            push_buffer.abandon(error)
        if all(item is not runtime for item in runtimes):
            runtimes = runtimes + (runtime,)
        for item in runtimes:
            abandon_actor(item)
        _, _, _, _, _, _, current_cell = self._publication_snapshot
        if current_cell is not failure_cell or not failure_cell.lock.acquire(blocking=False):
            return
        try:
            if failure_cell.failure is None:
                failure_cell.failure = (runtime, error)
        finally:
            failure_cell.lock.release()

    def settle_published_failure(
        self,
        *,
        pool_epoch: int,
        broker: LeaseBroker | None,
        deadline: float | None = None,
    ) -> None:
        state, epoch, current_broker, _, _, runtimes, failure_cell = self._publication_snapshot
        publication = failure_cell.failure
        if publication is None:
            publication = next(
                (
                    (runtime, runtime.fatal_error)
                    for runtime in runtimes
                    if runtime.runtime_epoch == pool_epoch and runtime.fatal_error is not None
                ),
                None,
            )
        if publication is None:
            publication = next(
                (
                    (handle._runtime, handle._error)
                    for handle in self._fatal_handle_snapshot
                    if handle.pool_epoch == pool_epoch
                    and handle.broker_ref() is broker
                    and handle._published.is_set()
                    and handle._runtime is not None
                    and handle._error is not None
                ),
                None,
            )
        if (
            publication is None
            or state not in (GuardState.ACTIVE, GuardState.SEALED)
            or epoch != pool_epoch
            or current_broker is not broker
            or failure_cell.pool_epoch != pool_epoch
            or failure_cell.broker is not broker
        ):
            return
        runtime, error = publication
        self.fail(
            runtime,
            error,
            pool_epoch=pool_epoch,
            broker=broker,
            deadline=deadline,
        )

    def seal(
        self,
        *,
        pool_epoch: int,
        broker: LeaseBroker | None,
        deadline: float | None = None,
    ) -> bool:
        if deadline is None:
            self._lock.acquire()
            acquired = True
        else:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool runtime guard seal blocked before deadline")
        try:
            if (
                self._state not in (GuardState.ACTIVE, GuardState.SEALED)
                or broker is None
                or broker is not self._broker
                or pool_epoch != self._epoch
            ):
                return False
            self._retire_requested.set()
            self._state = GuardState.SEALED
            self._refresh_publication_snapshot_locked()
            return True
        finally:
            self._lock.release()

    def request_seal(self) -> None:
        self._retire_requested.set()

    def request_stop(
        self,
        *,
        pool_epoch: int,
        broker: LeaseBroker,
        deadline: float | None = None,
    ) -> None:
        if deadline is None:
            self._lock.acquire()
            acquired = True
        else:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool runtime guard stop snapshot blocked before deadline")
        try:
            if broker is not self._broker or pool_epoch != self._epoch:
                return
            runtimes = tuple(self._runtimes)
        finally:
            self._lock.release()
        first_error: BaseException | None = None
        for runtime in runtimes:
            try:
                if deadline is None:
                    request_actor_stop(runtime)
                else:
                    request_actor_stop(runtime, deadline=deadline)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def abandon(self) -> None:
        self._retire_requested.set()
        if self._state is GuardState.ACTIVE:
            self._state = GuardState.SEALED
        broker = self._broker
        push_buffer = self._push_buffer
        runtimes = tuple(self._runtimes)
        if broker is not None:
            try:
                broker.abandon()
            except BaseException:
                pass
        if push_buffer is not None:
            try:
                push_buffer.abandon()
            except BaseException:
                pass
        for runtime in runtimes:
            try:
                abandon_actor(runtime)
            except BaseException:
                pass

    def failure(self, *, deadline: float | None = None) -> BaseException | None:
        state, epoch, broker, _, _, runtimes, failure_cell = self._publication_snapshot
        publication = failure_cell.failure
        published = (
            publication[1]
            if publication is not None
            and failure_cell.pool_epoch == epoch
            and failure_cell.broker is broker
            and state in (GuardState.ACTIVE, GuardState.SEALED)
            else None
        )
        if published is not None:
            return published
        runtime_fatal = next(
            (
                runtime.fatal_error
                for runtime in runtimes
                if runtime.runtime_epoch == epoch and runtime.fatal_error is not None
            ),
            None,
        )
        if runtime_fatal is not None:
            return runtime_fatal
        handle_fatal = next(
            (
                handle._error
                for handle in self._fatal_handle_snapshot
                if handle.pool_epoch == epoch
                and handle.broker_ref() is broker
                and handle._published.is_set()
                and handle._error is not None
            ),
            None,
        )
        if handle_fatal is not None:
            return handle_fatal
        if deadline is None:
            self._lock.acquire()
            acquired = True
        else:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool runtime guard inspection blocked before deadline")
        try:
            return self._fatal_error or published or runtime_fatal
        finally:
            self._lock.release()

    def finish_epoch(
        self,
        *,
        pool_epoch: int,
        broker: LeaseBroker,
        deadline: float | None = None,
    ) -> BaseException | None:
        if deadline is None:
            self._lock.acquire()
            acquired = True
        else:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool runtime guard finish blocked before deadline")
        try:
            if broker is not self._broker or pool_epoch != self._epoch:
                return self._fatal_error
            error = self._fatal_error
            publication = self._failure_cell.failure
            if (
                error is None
                and publication is not None
                and self._failure_cell.pool_epoch == pool_epoch
                and self._failure_cell.broker is broker
            ):
                error = publication[1]
            if error is None:
                error = next(
                    (
                        runtime.fatal_error
                        for runtime in self._runtime_snapshot
                        if runtime.runtime_epoch == pool_epoch and runtime.fatal_error is not None
                    ),
                    None,
                )
            if error is None:
                error = next(
                    (
                        handle._error
                        for handle in self._fatal_handle_snapshot
                        if handle.pool_epoch == pool_epoch
                        and handle.broker_ref() is broker
                        and handle._published.is_set()
                        and handle._error is not None
                    ),
                    None,
                )
            if error is not None:
                self._fatal_error = error
                self._state = GuardState.SEALED
                self._refresh_publication_snapshot_locked()
                return error
            self._broker = None
            self._push_buffer = None
            self._runtimes = []
            self._runtime_snapshot = ()
            self._fatal_handle_snapshot = ()
            self._fatal_error = None
            self._epoch = None
            self._state = GuardState.INACTIVE
            self._failure_cell = _GuardFailurePublication(None, None)
            self._refresh_publication_snapshot_locked()
            return None
        finally:
            self._lock.release()

    def register_fatal_handle(self, handle: ActorFatalHandle) -> bool:
        with self._lock:
            accepted = (
                self._state in (GuardState.ACTIVE, GuardState.SEALED)
                and handle.pool_epoch == self._epoch
                and handle.broker_ref() is self._broker
            )
            if accepted and all(item is not handle for item in self._fatal_handle_snapshot):
                self._fatal_handle_snapshot = self._fatal_handle_snapshot + (handle,)
            return accepted

    def cleanup_failure(self, *, deadline: float | None = None) -> BaseException | None:
        state, epoch, broker, _, _, _, failure_cell = self._publication_snapshot
        if (
            state not in (GuardState.ACTIVE, GuardState.SEALED)
            or failure_cell.pool_epoch != epoch
            or failure_cell.broker is not broker
        ):
            return None
        if deadline is None:
            failure_cell.lock.acquire()
            acquired = True
        else:
            acquired = failure_cell.lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool cleanup inspection blocked before deadline")
        try:
            return failure_cell.cleanup_error
        finally:
            failure_cell.lock.release()

    def clear_cleanup_failure(
        self,
        *,
        pool_epoch: int,
        broker: LeaseBroker,
        deadline: float | None = None,
    ) -> bool:
        state, epoch, current_broker, _, _, _, failure_cell = self._publication_snapshot
        if (
            state not in (GuardState.ACTIVE, GuardState.SEALED)
            or epoch != pool_epoch
            or current_broker is not broker
            or failure_cell.pool_epoch != pool_epoch
            or failure_cell.broker is not broker
        ):
            return False
        if deadline is None:
            failure_cell.lock.acquire()
            acquired = True
        else:
            acquired = failure_cell.lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool cleanup clear blocked before deadline")
        try:
            failure_cell.cleanup_error = None
            return True
        finally:
            failure_cell.lock.release()

    def _refresh_publication_snapshot_locked(self) -> None:
        self._publication_snapshot = (
            self._state,
            self._epoch,
            self._broker,
            self._retire_requested,
            self._push_buffer,
            self._runtime_snapshot,
            self._failure_cell,
        )


@dataclass(frozen=True, slots=True)
class ActorFatalHandle:
    guard_ref: weakref.ReferenceType[PoolRuntimeGuard]
    pool_epoch: int
    broker_ref: weakref.ReferenceType[LeaseBroker]
    retire_event: threading.Event
    _published: threading.Event = field(default_factory=threading.Event, compare=False)
    _runtime: ActorRuntime | None = field(default=None, compare=False)
    _error: BaseException | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        guard = self.guard_ref()
        if guard is not None:
            guard.register_fatal_handle(self)

    def publish(self, runtime: ActorRuntime, error: BaseException) -> None:
        object.__setattr__(self, "_runtime", runtime)
        if self._error is None:
            object.__setattr__(self, "_error", error)
        self._published.set()
        self.retire_event.set()
        guard = self.guard_ref()
        if guard is None:
            return
        guard.publish_failure(
            runtime,
            error,
            pool_epoch=self.pool_epoch,
            broker=self.broker_ref(),
        )

    publish_nonblocking = publish

    def settle(self, runtime: ActorRuntime | None = None, *, deadline: float | None = None) -> None:
        if not self._published.is_set():
            return
        runtime = runtime or self._runtime
        error = self._error
        guard = self.guard_ref()
        if runtime is None or error is None or guard is None:
            return
        guard.fail(
            runtime,
            error,
            pool_epoch=self.pool_epoch,
            broker=self.broker_ref(),
            deadline=deadline if deadline is not None else getattr(error, "_eltdx_deadline", None),
        )

    def __call__(self, runtime: ActorRuntime, error: BaseException) -> None:
        self.publish(runtime, error)
        self.settle(runtime)


@dataclass(frozen=True, slots=True)
class RuntimeRegistration:
    guard_ref: weakref.ReferenceType[PoolRuntimeGuard]
    pool_epoch: int
    broker_ref: weakref.ReferenceType[LeaseBroker]
    retire_event: threading.Event | None = None

    def __call__(self, runtime: ActorRuntime) -> bool:
        return self.register(runtime)

    def register(self, runtime: ActorRuntime, *, deadline: float | None = None) -> bool:
        if self.retire_event is not None and self.retire_event.is_set():
            request_actor_stop(runtime, deadline=deadline)
            return False
        guard = self.guard_ref()
        if guard is None:
            if deadline is None:
                request_actor_stop(runtime)
            else:
                request_actor_stop(runtime, deadline=deadline)
            return False
        try:
            return guard.add_runtime(
                runtime,
                pool_epoch=self.pool_epoch,
                broker=self.broker_ref(),
                deadline=deadline,
            )
        except BaseException:
            if self.retire_event is not None:
                self.retire_event.set()
            raise

    def is_active(self, *, deadline: float | None = None) -> bool:
        if self.retire_event is not None and self.retire_event.is_set():
            return False
        guard = self.guard_ref()
        return guard is not None and guard.is_active(
            pool_epoch=self.pool_epoch,
            broker=self.broker_ref(),
            deadline=deadline,
        )


def _abandon_pool(guard: PoolRuntimeGuard) -> None:
    guard.abandon()


@dataclass(frozen=True, slots=True)
class PoolDiagnostics:
    epoch: int
    state: PoolState
    broker: BrokerSnapshot | None
    actors: tuple[ActorSnapshot, ...]
    push_frames: int
    push_bytes: int
    push_dropped: int


@dataclass(slots=True)
class ShutdownAttempt:
    generation: int
    deadline: float
    broker: LeaseBroker | None = None
    push_buffer: PushBuffer | None = None
    connect_futures: tuple[Future[Any], ...] = ()
    connect_executor: ThreadPoolExecutor | None = None
    completed: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    forced_error: BaseException | None = None


@dataclass(slots=True)
class StartupAttempt:
    pool_epoch: int
    retire_event: threading.Event
    dns_preflight: bool = False
    dns_started_at: float | None = None
    dns_completed_at: float | None = None
    request_deadline: float | None = None
    dns_ready: threading.Event = field(default_factory=threading.Event)
    broker: LeaseBroker | None = None
    push_buffer: PushBuffer | None = None
    configured: list[tuple[SocketTransport, RuntimeRegistration]] = field(default_factory=list)
    completed: threading.Event = field(default_factory=_INTERNAL_EVENT)
    cleanup_complete: bool = False
    error: BaseException | None = None


@dataclass(slots=True)
class PinWaiter:
    call_id: int
    deadline: float
    completed: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    reserved: bool = False
    assigned: bool = False
    cancelled: threading.Event = field(default_factory=_INTERNAL_EVENT)


@dataclass(slots=True)
class PinCloseAttempt:
    deadline: float
    completed: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    timeout_error: TransportCloseTimeoutError = field(
        default_factory=lambda: TransportCloseTimeoutError("pinned transport close timed out")
    )


@dataclass(frozen=True, slots=True)
class _PinActiveCall:
    call_id: int
    terminal: threading.Event = field(default_factory=_INTERNAL_EVENT, compare=False)


class PinCompletion:
    propagate_settlement_error = True

    def __init__(self, proxy: PinnedTransportProxy, call_id: int, deadline: float | None = None) -> None:
        self._proxy: PinnedTransportProxy | None = proxy
        self._call_id = call_id
        self._deadline = deadline
        self._settle_lock = threading.Lock()
        self._done = False

    def publish(self, ticket: object | None) -> None:
        if self._done:
            return
        self._done = True
        proxy = self._proxy
        if proxy is not None:
            proxy._publish_wire_terminal(self._call_id)

    publish_nonblocking = publish

    def settle(self, ticket: object | None = None) -> None:
        with self._settle_lock:
            proxy = self._proxy
            try:
                if proxy is not None:
                    proxy._wire_terminal(self._call_id, deadline=self._deadline)
            finally:
                self._proxy = None

    def __call__(self, ticket: object | None) -> None:
        self.publish(ticket)
        self.settle(ticket)


class PinnedTransportProxy:
    def __init__(
        self,
        broker: LeaseBroker,
        lease: SlotLease,
        slot: SocketTransport,
        push_buffer: PushBuffer,
        timeout: float,
    ) -> None:
        self._broker = broker
        self._lease = lease
        self._slot = slot
        self._push_buffer = push_buffer
        self._timeout = timeout
        self._condition = threading.Condition()
        self._waiters: deque[PinWaiter] = deque()
        self._waiter_snapshot: tuple[PinWaiter, ...] = ()
        self._close_waiters: list[PinWaiter] = []
        self._call_counter = 0
        self._active_call_state: _PinActiveCall | None = None
        self._active_waiter: PinWaiter | None = None
        self._wire_call: int | None = None
        self._state = PinState.OPEN
        self._close_requested = threading.Event()
        self._close_attempt: PinCloseAttempt | None = None

    @property
    def _active_call(self) -> int | None:
        state = self._active_call_state
        return state.call_id if state is not None else None

    @_active_call.setter
    def _active_call(self, call_id: int | None) -> None:
        state = self._active_call_state
        if state is not None and state.call_id == call_id:
            return
        self._active_call_state = _PinActiveCall(call_id) if call_id is not None else None

    def _terminal_published(self) -> bool:
        state = self._active_call_state
        return state is not None and state.terminal.is_set()

    def _admission_terminal(self) -> bool:
        return (
            self._broker.retired
            or self._close_requested.is_set()
            or self._lease.state is not LeaseState.ACTIVE
            or (
                self._lease.cancellation is not None
                and self._lease.cancellation.is_set()
            )
            or self._terminal_published()
        )

    def _acquire_condition(self, deadline: float, *, closing: bool = False) -> None:
        if self._condition.acquire(timeout=max(0.0, deadline - time.monotonic())):
            return
        if closing:
            raise TransportCloseTimeoutError("pinned transport state blocked before close deadline")
        raise ResponseTimeoutError("7709 response timed out during pinned admission")

    @property
    def connected_host(self) -> str | None:
        self._validate()
        return self._slot.connected_host

    @property
    def last_handshake(self) -> Any:
        self._validate()
        return self._slot.last_handshake

    @property
    def last_heartbeat(self) -> Any:
        self._validate()
        return self._slot.last_heartbeat

    @property
    def pending_push_count(self) -> int:
        self._validate()
        return self._push_buffer.pending_count

    def connect(self) -> None:
        deadline = time.monotonic() + self._timeout
        call_id = self._admit(deadline)
        try:
            completion = PinCompletion(self, call_id, deadline)
        except BaseException:
            self._wire_terminal(call_id)
            raise
        runtime = self._slot._runtime
        self._slot._connect_with_deadline(
            deadline=deadline,
            completion=completion,
            runtime=runtime,
            lock_slot=False,
            lease_id=self._lease.lease_id,
            expected_runtime_epoch=self._lease.pool_epoch,
            submission_check=lambda: self._begin_wire(call_id, deadline=deadline),
        )

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        deadline = time.monotonic() + self._timeout
        call_id = self._admit(deadline)
        try:
            completion = PinCompletion(self, call_id, deadline)
        except BaseException:
            self._wire_terminal(call_id)
            raise
        runtime = self._slot._runtime
        return self._slot._execute_with_lease(
            command,
            payload,
            lease_id=self._lease.lease_id,
            deadline=deadline,
            completion=completion,
            runtime=runtime,
            lock_slot=False,
            expected_runtime_epoch=self._lease.pool_epoch,
            submission_check=lambda: self._begin_wire(call_id, deadline=deadline),
        )

    def request(self, command: str) -> str:
        self._validate()
        if command == "ping":
            return "pong"
        raise ValueError(f"unsupported command: {command}")

    def poll_push(self, timeout: float | None = 0.0, *, parse: bool = False) -> Any:
        self._validate()
        item = self._push_buffer.poll(timeout)
        if item is None or not parse:
            return item.response if item is not None else None
        return _parse_push(item)

    def drain_pushes(self, *, parse: bool = False) -> list[Any]:
        self._validate()
        items = self._push_buffer.drain()
        return [_parse_push(item) for item in items] if parse else [item.response for item in items]

    def close(self) -> None:
        deadline = time.monotonic() + min(1.0, self._timeout)
        self._close_requested.set()
        self._settle_published_terminal(deadline)
        self._acquire_condition(deadline, closing=True)
        try:
            if self._state is PinState.CLOSED:
                return
            attempt = self._close_attempt
            owner = (
                self._state is not PinState.CLOSING
                or attempt is None
                or attempt.completed.is_set()
            )
            if owner:
                attempt = PinCloseAttempt(deadline)
                self._close_attempt = attempt
                self._state = PinState.CLOSING
        finally:
            self._condition.release()
        if not owner:
            if not attempt.completed.wait(max(0.0, deadline - time.monotonic())):
                if not attempt.completed.is_set():
                    raise attempt.timeout_error
            if attempt.error is not None:
                raise attempt.error
            return
        gate_acquired = False
        submission_token = object()
        try:
            self._acquire_condition(deadline, closing=True)
            try:
                while self._waiters:
                    waiter = self._waiters.popleft()
                    waiter.error = ConnectionClosedError("pinned transport closed")
                    self._close_waiters.append(waiter)
                closed_waiters = tuple(self._close_waiters)
            finally:
                self._condition.release()
            waiter_cleanup_error: BaseException | None = None
            for waiter in closed_waiters:
                try:
                    if waiter.reserved:
                        self._broker.release_pin_waiter(waiter.completed, deadline=deadline)
                        waiter.reserved = False
                except BaseException as exc:
                    if waiter_cleanup_error is None:
                        waiter_cleanup_error = exc
                finally:
                    waiter.completed.set()
            self._acquire_condition(deadline, closing=True)
            try:
                self._close_waiters = [waiter for waiter in self._close_waiters if waiter.reserved]
                self._refresh_waiter_snapshot_locked()
            finally:
                self._condition.release()
            if waiter_cleanup_error is not None:
                raise waiter_cleanup_error
            if self._close_waiters:
                raise TransportCloseTimeoutError("pinned waiter reservations were not released")
            remaining = max(0.0, deadline - time.monotonic())
            try:
                gate_acquired = _acquire_gate_token(
                    self._slot._submission_gate,
                    submission_token,
                    deadline,
                )
            except BaseException:
                _release_gate_token(self._slot._submission_gate, submission_token)
                raise
            if not gate_acquired:
                raise TransportCloseTimeoutError("pinned transport submission did not quiesce")
            self._acquire_condition(deadline, closing=True)
            try:
                active = self._active_call
                wire_started = active is not None and self._wire_call == active
                if active is not None and not wire_started:
                    self._active_call = None
                    self._active_waiter = None
                    self._condition.notify_all()
            finally:
                self._condition.release()
            if wire_started:
                self._slot._cancel_lease(self._lease.lease_id, deadline=deadline)
            _release_gate_token(self._slot._submission_gate, submission_token)
            gate_acquired = False
            if wire_started:
                self._acquire_condition(deadline, closing=True)
                try:
                    if not self._condition.wait_for(
                        lambda: self._active_call is None,
                        timeout=max(0.0, deadline - time.monotonic()),
                    ):
                        raise TransportCloseTimeoutError("pinned transport did not quiesce")
                finally:
                    self._condition.release()
            if not self._broker.release(self._lease, deadline=deadline):
                if self._lease.state is not LeaseState.RELEASED:
                    raise TransportCloseTimeoutError("pinned transport lease was not released")
        except BaseException as exc:
            if gate_acquired:
                _release_gate_token(self._slot._submission_gate, submission_token)
            release_failed_lease = False
            published_error = attempt.timeout_error if isinstance(exc, TransportCloseTimeoutError) else exc
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if acquired:
                try:
                    self._state = PinState.FAILED
                    release_failed_lease = self._active_call is None
                    if attempt.error is None:
                        attempt.error = published_error
                    published_error = attempt.error
                    self._condition.notify_all()
                finally:
                    self._condition.release()
            elif attempt.error is None:
                attempt.error = published_error
            attempt.completed.set()
            if release_failed_lease:
                try:
                    self._release_failed_lease(deadline=deadline)
                except BaseException:
                    pass
            raise published_error
        acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            error = TransportCloseTimeoutError("pinned transport close completion blocked before deadline")
            attempt.error = error
            attempt.completed.set()
            raise error
        try:
            self._state = PinState.CLOSED
            self._condition.notify_all()
        finally:
            self._condition.release()
            attempt.completed.set()

    def _begin_wire(self, call_id: int, *, deadline: float) -> None:
        try:
            lease_valid = self._broker.validate(self._lease, deadline=deadline)
        except BaseException:
            self._withdraw_unstarted_call(call_id, deadline)
            raise
        self._acquire_condition(deadline)
        try:
            valid = (
                self._state is PinState.OPEN
                and not self._close_requested.is_set()
                and self._active_call == call_id
                and lease_valid
                and not self._broker.retired
            )
            if not valid:
                raise ConnectionClosedError("pinned transport closed before wire submission")
            self._wire_call = call_id
            self._active_waiter = None
            self._refresh_waiter_snapshot_locked()
        finally:
            self._condition.release()

    def _validate(self) -> None:
        with self._condition:
            open_state = self._state is PinState.OPEN and not self._close_requested.is_set()
        if not open_state or not self._broker.validate(self._lease):
            raise ConnectionClosedError("pinned transport lease is no longer valid")

    def _admit(self, deadline: float) -> int:
        self._settle_published_terminal(deadline)
        self._reap_cancelled_active(deadline)
        lease_valid = self._broker.validate(self._lease, deadline=deadline)
        self._acquire_condition(deadline)
        try:
            if self._state is not PinState.OPEN or self._close_requested.is_set() or not lease_valid:
                raise ConnectionClosedError("pinned transport lease is no longer valid")
            if time.monotonic() >= deadline:
                raise ResponseTimeoutError("7709 response timed out during queue")
            self._call_counter += 1
            call_id = self._call_counter
            if self._active_call is None and not self._waiters:
                self._active_call = call_id
                self._active_waiter = None
                if self._admission_terminal():
                    self._active_call = None
                    raise ConnectionClosedError("pinned transport closed during admission")
                return call_id
            waiter = PinWaiter(call_id, deadline)
            self._broker.reserve_pin_waiter(
                waiter.completed,
                cancelled=waiter.cancelled,
                deadline=deadline,
            )
            waiter.reserved = True
            self._waiters.append(waiter)
            self._refresh_waiter_snapshot_locked()
            if self._admission_terminal():
                waiter.completed.set()
        finally:
            self._condition.release()
        while True:
            try:
                woke = waiter.completed.wait(max(0.0, deadline - time.monotonic()))
            except BaseException:
                self._cancel_interrupted_waiter(waiter, deadline)
                raise
            if not woke:
                break
            published_wake = self._terminal_published()
            if published_wake:
                self._settle_published_terminal(deadline)
            self._acquire_condition(deadline)
            try:
                if waiter.assigned or waiter.error is not None:
                    break
                lease_terminal = (
                    self._broker.retired
                    or self._state is not PinState.OPEN
                    or self._close_requested.is_set()
                    or self._lease.state is not LeaseState.ACTIVE
                    or (
                        self._lease.cancellation is not None
                        and self._lease.cancellation.is_set()
                    )
                )
                if lease_terminal:
                    break
                waiter.completed.clear()
                if self._admission_terminal():
                    waiter.completed.set()
            finally:
                self._condition.release()
        if not woke:
            waiter.cancelled.set()
            self._acquire_condition(deadline)
            try:
                if waiter.assigned:
                    pass
                elif not waiter.completed.is_set():
                    try:
                        self._waiters.remove(waiter)
                    except ValueError:
                        pass
                    waiter.error = ResponseTimeoutError("7709 response timed out during queue")
            finally:
                self._condition.release()
            if waiter.reserved:
                self._broker.release_pin_waiter(waiter.completed, deadline=deadline)
                waiter.reserved = False
        if waiter.reserved:
            try:
                self._broker.release_pin_waiter(waiter.completed, deadline=deadline)
                waiter.reserved = False
            except BaseException:
                waiter.cancelled.set()
                waiter.reserved = False
                self._withdraw_unstarted_call(call_id, deadline)
                raise
        try:
            lease_valid = self._broker.validate(self._lease, deadline=deadline)
        except BaseException:
            self._withdraw_unstarted_call(call_id, deadline)
            raise
        release_failed_lease = False
        closed_waiters: list[PinWaiter] = []
        self._acquire_condition(deadline)
        try:
            if waiter.error is None:
                valid = (
                    waiter.assigned
                    and self._active_call == call_id
                    and self._state is PinState.OPEN
                    and not self._close_requested.is_set()
                    and not waiter.cancelled.is_set()
                    and lease_valid
                    and time.monotonic() < deadline
                    and not self._broker.retired
                )
                if not valid:
                    waiter.error = (
                        ResponseTimeoutError("7709 response timed out during queue")
                        if time.monotonic() >= deadline
                        else ConnectionClosedError("pinned transport closed during admission")
                    )
                    waiter.assigned = False
                    if self._active_call == call_id:
                        self._active_call = None
                        if self._active_waiter is waiter:
                            self._active_waiter = None
                        release_failed_lease = self._state is PinState.FAILED
                    if self._state is PinState.OPEN and not lease_valid:
                        self._state = PinState.CLOSED
                    if self._state is not PinState.OPEN or not lease_valid:
                        closed_waiters.extend(self._close_waiters_locked())
                    self._refresh_waiter_snapshot_locked()
                    self._condition.notify_all()
            error = waiter.error
        finally:
            self._condition.release()
        for closed_waiter in closed_waiters:
            if closed_waiter.reserved:
                try:
                    self._broker.release_pin_waiter(
                        closed_waiter.completed,
                        deadline=deadline,
                    )
                    closed_waiter.reserved = False
                except BaseException:
                    pass
            closed_waiter.completed.set()
        if release_failed_lease:
            self._release_failed_lease(deadline=deadline)
        if error is not None:
            raise error
        return call_id

    def _withdraw_unstarted_call(self, call_id: int, deadline: float) -> None:
        acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            self._close_requested.set()
            for waiter in self._waiter_snapshot:
                if waiter.error is None:
                    waiter.error = ConnectionClosedError("pinned transport failed during admission cleanup")
                waiter.completed.set()
            return
        wake: list[PinWaiter] = []
        try:
            if self._active_call == call_id and self._wire_call != call_id:
                self._active_call = None
                self._active_waiter = None
                while self._waiters:
                    candidate = self._waiters.popleft()
                    if candidate.error is not None:
                        wake.append(candidate)
                        continue
                    if candidate.cancelled.is_set() or time.monotonic() >= candidate.deadline:
                        candidate.error = ResponseTimeoutError("7709 response timed out during queue")
                        wake.append(candidate)
                        continue
                    self._active_call = candidate.call_id
                    self._active_waiter = candidate
                    candidate.assigned = True
                    wake.append(candidate)
                    break
                self._refresh_waiter_snapshot_locked()
                self._condition.notify_all()
        finally:
            self._condition.release()
        for candidate in wake:
            if candidate.reserved:
                try:
                    self._broker.release_pin_waiter(
                        candidate.completed,
                        deadline=time.monotonic(),
                    )
                    candidate.reserved = False
                except BaseException:
                    pass
            candidate.completed.set()

    def _wire_terminal(self, call_id: int, *, deadline: float | None = None) -> None:
        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            self._publish_wire_terminal(call_id)
            raise ResponseTimeoutError("7709 response timed out settling pinned completion")
        wake: list[PinWaiter] = []
        release_failed_lease = False
        try:
            if self._active_call != call_id:
                return
            self._active_call = None
            self._active_waiter = None
            self._wire_call = None
            if self._state is PinState.FAILED:
                release_failed_lease = True
            elif (
                self._state is not PinState.OPEN
                or self._broker.retired
                or self._close_requested.is_set()
                or self._lease.state is not LeaseState.ACTIVE
                or (self._lease.cancellation is not None and self._lease.cancellation.is_set())
            ):
                if self._state is PinState.OPEN:
                    self._state = PinState.FAILED if self._close_requested.is_set() else PinState.CLOSED
                if self._state is PinState.FAILED:
                    release_failed_lease = True
                wake.extend(self._close_waiters_locked())
            else:
                while self._waiters:
                    candidate = self._waiters.popleft()
                    if candidate.error is None:
                        if candidate.cancelled.is_set() or time.monotonic() >= candidate.deadline:
                            candidate.error = ResponseTimeoutError("7709 response timed out during queue")
                            wake.append(candidate)
                            continue
                        self._active_call = candidate.call_id
                        self._active_waiter = candidate
                        candidate.assigned = True
                        wake.append(candidate)
                        break
            self._refresh_waiter_snapshot_locked()
            self._condition.notify_all()
        finally:
            self._condition.release()
        for candidate in wake:
            if candidate.reserved:
                try:
                    self._broker.release_pin_waiter(
                        candidate.completed,
                        deadline=time.monotonic(),
                    )
                    candidate.reserved = False
                except BaseException:
                    pass
            candidate.completed.set()
        if release_failed_lease:
            try:
                self._release_failed_lease(deadline=time.monotonic())
            except BaseException:
                pass

    def _publish_wire_terminal(self, call_id: int) -> None:
        state = self._active_call_state
        if state is None or state.call_id != call_id:
            return
        state.terminal.set()
        for waiter in self._waiter_snapshot:
            waiter.completed.set()

    def _settle_published_terminal(self, deadline: float) -> None:
        state = self._active_call_state
        if state is None or not state.terminal.is_set():
            return
        call_id = state.call_id
        if not self._condition.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise ResponseTimeoutError("7709 response timed out settling pinned completion")
        self._condition.release()
        self._wire_terminal(call_id, deadline=deadline)
        state.terminal.clear()

    def _cancel_interrupted_waiter(self, waiter: PinWaiter, deadline: float) -> None:
        waiter.cancelled.set()
        wake: list[PinWaiter] = []
        acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if acquired:
            try:
                try:
                    self._waiters.remove(waiter)
                except ValueError:
                    pass
                waiter.error = ConnectionClosedError("pinned transport admission interrupted")
                if self._active_call == waiter.call_id and self._wire_call != waiter.call_id:
                    self._active_call = None
                    self._active_waiter = None
                    while self._waiters:
                        candidate = self._waiters.popleft()
                        if candidate.cancelled.is_set() or time.monotonic() >= candidate.deadline:
                            candidate.error = ResponseTimeoutError("7709 response timed out during queue")
                            wake.append(candidate)
                            continue
                        self._active_call = candidate.call_id
                        self._active_waiter = candidate
                        candidate.assigned = True
                        wake.append(candidate)
                        break
                self._refresh_waiter_snapshot_locked()
                self._condition.notify_all()
            finally:
                self._condition.release()
        if waiter.reserved:
            try:
                self._broker.release_pin_waiter(waiter.completed, deadline=deadline)
                waiter.reserved = False
            except BaseException:
                pass
        waiter.completed.set()
        for candidate in wake:
            if candidate.reserved:
                try:
                    self._broker.release_pin_waiter(candidate.completed, deadline=deadline)
                    candidate.reserved = False
                except BaseException:
                    pass
            candidate.completed.set()

    def _reap_cancelled_active(self, deadline: float) -> None:
        wake: list[PinWaiter] = []
        self._acquire_condition(deadline)
        try:
            waiter = self._active_waiter
            if (
                waiter is None
                or not waiter.cancelled.is_set()
                or self._wire_call == waiter.call_id
            ):
                return
            if self._active_call == waiter.call_id:
                self._active_call = None
            self._active_waiter = None
            while self._waiters:
                candidate = self._waiters.popleft()
                if candidate.cancelled.is_set() or time.monotonic() >= candidate.deadline:
                    candidate.error = ResponseTimeoutError("7709 response timed out during queue")
                    wake.append(candidate)
                    continue
                self._active_call = candidate.call_id
                self._active_waiter = candidate
                candidate.assigned = True
                wake.append(candidate)
                break
            self._refresh_waiter_snapshot_locked()
            self._condition.notify_all()
        finally:
            self._condition.release()
        for candidate in wake:
            if candidate.reserved:
                try:
                    self._broker.release_pin_waiter(candidate.completed, deadline=deadline)
                    candidate.reserved = False
                except BaseException:
                    pass
            candidate.completed.set()

    def _close_waiters_locked(self) -> list[PinWaiter]:
        wake: list[PinWaiter] = []
        while self._waiters:
            waiter = self._waiters.popleft()
            if waiter.error is None:
                waiter.error = ConnectionClosedError("pinned transport closed during admission")
            wake.append(waiter)
        self._refresh_waiter_snapshot_locked()
        return wake

    def _refresh_waiter_snapshot_locked(self) -> None:
        active = (
            (self._active_waiter,)
            if self._active_waiter is not None and self._active_waiter.reserved
            else ()
        )
        self._waiter_snapshot = active + tuple(self._waiters) + tuple(
            waiter for waiter in self._close_waiters if waiter.reserved
        )

    def _release_failed_lease(self, *, deadline: float | None = None) -> None:
        try:
            self._broker.release(self._lease, deadline=deadline)
        except BaseException:
            try:
                _mark_lease_cancelled(self._lease)
            except BaseException:
                pass
            raise
        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("pinned transport state blocked lease release")
        try:
            if self._state is PinState.FAILED and self._active_call is None:
                self._state = PinState.CLOSED
            self._condition.notify_all()
        finally:
            self._condition.release()


class PooledSocketTransport:
    def __init__(
        self,
        hosts: Sequence[str] | None = None,
        *,
        timeout: float = 8.0,
        pool_size: int = 2,
        probe_hosts: bool = False,
        probe_timeout: float = DEFAULT_PROBE_TIMEOUT,
        probe_workers: int = DEFAULT_PROBE_WORKERS,
        heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL,
        max_pending_requests: int = DEFAULT_MAX_PENDING_REQUESTS,
        push_queue_size: int = DEFAULT_PUSH_QUEUE_SIZE,
        push_queue_bytes: int = DEFAULT_PUSH_QUEUE_BYTES,
    ) -> None:
        resolved_hosts = unique_hosts(list(hosts or DEFAULT_HOSTS))
        if not resolved_hosts:
            raise ValueError("at least one host is required")
        if probe_hosts and len(resolved_hosts) > 1:
            resolved_hosts = sort_hosts_by_latency(resolved_hosts, timeout=probe_timeout, max_workers=probe_workers)
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        if max_pending_requests <= 0:
            raise ValueError("max_pending_requests must be > 0")

        self._hosts = resolved_hosts
        self._timeout = float(timeout)
        self._pool_size = max(1, int(pool_size))
        self._heartbeat_interval = heartbeat_interval
        self._max_pending_requests = int(max_pending_requests)
        self._push_queue_size = int(push_queue_size)
        self._push_queue_bytes = int(push_queue_bytes)
        self._transports = [self._new_transport(index) for index in range(self._pool_size)]
        self._condition = threading.Condition()
        self._state = PoolState.STOPPED
        self._shutdown_failed = threading.Event()
        self._epoch_retire_event: threading.Event | None = None
        self._epoch = 0
        self._startup_active = False
        self._startup_aborted = threading.Event()
        self._startup_attempt: StartupAttempt | None = None
        self._published_startup_attempt: StartupAttempt | None = None
        self._startup_observer_lock = threading.Lock()
        self._observable_startup_attempt: StartupAttempt | None = None
        self._startup_cleanup_error: BaseException | None = None
        self._connect_broker: LeaseBroker | None = None
        self._connect_futures: tuple[Future[Any], ...] = ()
        self._connect_executor: ThreadPoolExecutor | None = None
        self._connect_shutdown_started = False
        self._shutdown_active = False
        self._shutdown_generation = 0
        self._shutdown_attempt: ShutdownAttempt | None = None
        self._broker: LeaseBroker | None = None
        self._push_buffer: PushBuffer | None = None
        self._registrations: tuple[RuntimeRegistration, ...] = ()
        self._runtime_guard = PoolRuntimeGuard()
        self._finalizer = weakref.finalize(self, _abandon_pool, self._runtime_guard)

    @property
    def hosts(self) -> tuple[str, ...]:
        return tuple(self._hosts)

    @property
    def pool_size(self) -> int:
        return self._pool_size

    @property
    def heartbeat_interval(self) -> float | None:
        return self._heartbeat_interval

    @property
    def max_pending_requests(self) -> int:
        return self._max_pending_requests

    @property
    def push_queue_size(self) -> int:
        return self._push_queue_size

    @property
    def push_queue_bytes(self) -> int:
        return self._push_queue_bytes

    @property
    def connected_hosts(self) -> tuple[str | None, ...]:
        return tuple(transport.connected_host for transport in self._transports)

    @property
    def connected_host(self) -> str | None:
        return next((host for host in self.connected_hosts if host is not None), None)

    @property
    def pending_push_count(self) -> int:
        with self._condition:
            push_buffer = self._push_buffer
        return push_buffer.pending_count if push_buffer is not None else 0

    @property
    def diagnostics(self) -> PoolDiagnostics:
        with self._condition:
            state = self._state
            epoch = self._epoch
            broker = self._broker
            push_buffer = self._push_buffer
        if self._shutdown_failed.is_set() and state is not PoolState.FAILED_CLOSED:
            state = PoolState.FAILED_CLOSING
        elif self._runtime_guard.failure() is not None and state is PoolState.RUNNING:
            state = PoolState.FAILED
        push = push_buffer.snapshot() if push_buffer is not None else None
        actors = tuple(
            actor_snapshot(runtime)
            for transport in self._transports
            if (runtime := transport._runtime) is not None
        )
        return PoolDiagnostics(
            epoch=epoch,
            state=state,
            broker=broker.snapshot() if broker is not None else None,
            actors=actors,
            push_frames=push.frame_count if push is not None else 0,
            push_bytes=push.byte_count if push is not None else 0,
            push_dropped=push.dropped_total if push is not None else 0,
        )

    def _shutdown_requested(self) -> bool:
        retire_event = self._epoch_retire_event
        return self._shutdown_failed.is_set() or (retire_event is not None and retire_event.is_set())

    def connect(self) -> None:
        connect_deadline = time.monotonic() + self._timeout
        broker, push_buffer, connect_deadline = self._ensure_started_before(connect_deadline)
        acquired = self._condition.acquire(timeout=max(0.0, connect_deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out during pool connect admission")
        try:
            while self._connect_broker is broker:
                if self._shutdown_requested() or self._state is not PoolState.RUNNING or self._broker is not broker:
                    raise ConnectionClosedError("7709 pool changed while waiting to connect")
                remaining = max(0.0, connect_deadline - time.monotonic())
                if remaining == 0 or not self._condition.wait(timeout=remaining):
                    raise ResponseTimeoutError("7709 response timed out during queue")
            if self._shutdown_requested() or self._state is not PoolState.RUNNING or self._broker is not broker:
                raise ConnectionClosedError("7709 pool changed before connect admission")
            self._connect_broker = broker
            connect_retire_event = self._epoch_retire_event
            connect_failure_event = self._shutdown_failed
        finally:
            self._condition.release()

        leases: list[SlotLease] = []
        connect_succeeded = False
        try:
            leases.extend(broker.acquire_many(self._pool_size, connect_deadline))
            lease_by_slot = {lease.slot_id: lease for lease in leases}
            first_error: BaseException | None = None
            stopped_early = False
            shutdown_attempt: ShutdownAttempt | None = None
            shutdown_owner = False
            rollback_errors: list[BaseException] = []
            shutdown_error: BaseException | None = None
            executor = ThreadPoolExecutor(max_workers=self._pool_size, thread_name_prefix="eltdx-pool-connect")
            futures: list[Future[Any]] = []
            try:
                submission_error: BaseException | None = None
                acquired = self._condition.acquire(timeout=max(0.0, connect_deadline - time.monotonic()))
                if not acquired:
                    raise ResponseTimeoutError("7709 response timed out publishing pool connect workers")
                try:
                    self._connect_executor = executor
                    self._connect_futures = ()
                    self._connect_shutdown_started = False
                finally:
                    self._condition.release()
                for index, transport in enumerate(self._transports):
                    try:
                        acquired = self._condition.acquire(timeout=max(0.0, connect_deadline - time.monotonic()))
                        if not acquired:
                            raise ResponseTimeoutError("7709 response timed out during pool connect submission")
                        try:
                            if self._shutdown_requested() or self._state is not PoolState.RUNNING or self._broker is not broker:
                                raise ConnectionClosedError("7709 pool changed during connect submission")
                            future = executor.submit(
                                transport._connect_with_deadline,
                                deadline=connect_deadline,
                                completion=None,
                                runtime=transport._runtime,
                                lock_slot=False,
                                lease_id=lease_by_slot[index].lease_id,
                                expected_runtime_epoch=broker.pool_epoch,
                            )
                            futures.append(future)
                            self._connect_futures = tuple(futures)
                            self._condition.notify_all()
                        finally:
                            self._condition.release()
                    except BaseException as exc:
                        submission_error = exc
                        break
                for future in futures:
                    future.add_done_callback(lambda _future, owned=executor: self._connect_future_terminal(owned))
                if submission_error is None:
                    done, pending = wait(
                        futures,
                        timeout=max(0.0, connect_deadline - time.monotonic()),
                        return_when=FIRST_EXCEPTION,
                    )
                    first_error = next((future.exception() for future in done if future.exception() is not None), None)
                else:
                    done = {future for future in futures if future.done()}
                    pending = set(futures) - done
                    first_error = submission_error
                if first_error is None and pending:
                    first_error = ResponseTimeoutError("7709 response timed out during pool connect")
                if first_error is not None:
                    try:
                        shutdown_attempt, shutdown_owner = self._claim_shutdown_attempt(
                            expected_broker=broker,
                            expected_push_buffer=push_buffer,
                            deadline=connect_deadline,
                        )
                    except BaseException:
                        connect_failure_event.set()
                        if connect_retire_event is not None:
                            connect_retire_event.set()
                        try:
                            self._runtime_guard.request_stop(
                                pool_epoch=broker.pool_epoch,
                                broker=broker,
                                deadline=connect_deadline,
                            )
                        except BaseException:
                            pass
                        raise
                    if shutdown_owner:
                        try:
                            self._begin_connect_rollback(broker, push_buffer, deadline=connect_deadline)
                        except BaseException as exc:
                            rollback_errors.append(exc)
                        stop_errors = _request_stop_transports(self._transports, connect_deadline)
                        rollback_errors.extend(stop_errors)
                        stopped_early = not stop_errors
                        try:
                            self._run_shutdown_attempt(
                                shutdown_attempt,
                                already_requested_stop=stopped_early,
                                initial_errors=rollback_errors,
                            )
                        except BaseException as exc:
                            shutdown_error = exc
                if shutdown_attempt is not None and not shutdown_owner:
                    try:
                        self._wait_shutdown_attempt(shutdown_attempt, deadline=connect_deadline)
                    except BaseException as exc:
                        shutdown_error = exc
                _, pending = wait(
                    futures,
                    timeout=max(0.0, connect_deadline - time.monotonic()),
                    return_when=ALL_COMPLETED,
                )
                if pending and shutdown_error is None:
                    shutdown_error = TransportCloseTimeoutError(
                        "7709 pool connect workers did not finish before connect deadline"
                    )
            finally:
                connect_cleanup_errors: list[BaseException] = []
                acquired = self._condition.acquire(timeout=max(0.0, connect_deadline - time.monotonic()))
                if acquired:
                    try:
                        if self._connect_executor is executor:
                            self._connect_shutdown_started = True
                    finally:
                        self._condition.release()
                else:
                    connect_cleanup_errors.append(
                        TransportCloseTimeoutError("7709 pool condition blocked connect cleanup before deadline")
                    )
                try:
                    executor.shutdown(wait=False, cancel_futures=True)
                except BaseException as exc:
                    connect_cleanup_errors.append(exc)
                executor_threads = _executor_worker_threads(executor)
                for thread in executor_threads:
                    thread.join(timeout=max(0.0, connect_deadline - time.monotonic()))
                if any(thread.is_alive() for thread in executor_threads):
                    connect_cleanup_errors.append(
                        TransportCloseTimeoutError("7709 pool connect executor did not stop before connect deadline")
                    )
                try:
                    self._connect_future_terminal(executor, deadline=connect_deadline)
                except BaseException as exc:
                    connect_cleanup_errors.append(exc)
                if connect_cleanup_errors:
                    connect_failure_event.set()
                    if connect_retire_event is not None:
                        connect_retire_event.set()
                    try:
                        self._runtime_guard.request_stop(
                            pool_epoch=broker.pool_epoch,
                            broker=broker,
                            deadline=connect_deadline,
                        )
                    except BaseException as exc:
                        connect_cleanup_errors.append(exc)
                    if shutdown_error is None:
                        shutdown_error = connect_cleanup_errors[0]
            errors = [future.exception() for future in futures if future.done() and future.exception() is not None]
            if errors:
                if shutdown_error is not None:
                    raise shutdown_error
                raise first_error or errors[0]
            if shutdown_error is not None:
                raise shutdown_error
            if first_error is not None:
                raise first_error
            connect_succeeded = True
        finally:
            final_cleanup_errors: list[BaseException] = []
            for lease in leases:
                try:
                    broker.release(lease, deadline=connect_deadline)
                except BaseException as exc:
                    final_cleanup_errors.append(exc)
            acquired = self._condition.acquire(timeout=max(0.0, connect_deadline - time.monotonic()))
            if acquired:
                try:
                    if connect_succeeded and (
                        self._broker is not broker
                        or self._state is not PoolState.RUNNING
                        or connect_failure_event.is_set()
                        or (connect_retire_event is not None and connect_retire_event.is_set())
                    ):
                        final_cleanup_errors.append(
                            ConnectionClosedError("7709 pool changed before connect completion")
                        )
                    if self._connect_broker is broker:
                        self._connect_broker = None
                    self._condition.notify_all()
                finally:
                    self._condition.release()
            else:
                final_cleanup_errors.append(
                    TransportCloseTimeoutError("7709 pool condition blocked connect release before deadline")
                )
            if final_cleanup_errors:
                connect_failure_event.set()
                if connect_retire_event is not None:
                    connect_retire_event.set()
                try:
                    self._runtime_guard.request_stop(
                        pool_epoch=broker.pool_epoch,
                        broker=broker,
                        deadline=connect_deadline,
                    )
                except BaseException as exc:
                    final_cleanup_errors.append(exc)
                raise final_cleanup_errors[0]

    def _connect_future_terminal(
        self,
        executor: ThreadPoolExecutor,
        *,
        deadline: float | None = None,
    ) -> None:
        if deadline is None:
            acquired = self._condition.acquire(blocking=False)
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            if deadline is None:
                return
            raise TransportCloseTimeoutError("7709 pool condition blocked connect terminal publication")
        try:
            if self._connect_executor is not executor or not all(
                future.done() for future in self._connect_futures
            ) or not self._connect_shutdown_started or any(
                thread.is_alive() for thread in _executor_worker_threads(executor)
            ):
                return
            self._connect_executor = None
            self._connect_futures = ()
            self._connect_shutdown_started = False
            self._condition.notify_all()
        finally:
            self._condition.release()

    def close(self) -> None:
        self._shutdown(normal=True)

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        deadline = time.monotonic() + self._timeout
        broker, _, deadline = self._ensure_started_before(deadline)
        lease = broker.acquire(deadline)
        try:
            completion = LeaseCompletion(broker, lease, deadline)
        except BaseException:
            try:
                broker.release(lease, deadline=deadline)
            except BaseException:
                _mark_lease_cancelled(lease)
            raise
        transport = self._transports[lease.slot_id]
        return transport._execute_with_lease(
            command,
            payload,
            lease_id=lease.lease_id,
            deadline=deadline,
            completion=completion,
            runtime=transport._runtime,
            lock_slot=False,
            expected_runtime_epoch=lease.pool_epoch,
        )

    @contextmanager
    def pin(self) -> Iterator[PinnedTransportProxy]:
        deadline = time.monotonic() + self._timeout
        broker, push_buffer, deadline = self._ensure_started_before(deadline)
        lease = broker.acquire(deadline, pinned=True)
        proxy = PinnedTransportProxy(
            broker,
            lease,
            self._transports[lease.slot_id],
            push_buffer,
            self._timeout,
        )
        try:
            yield proxy
        finally:
            proxy.close()

    def request(self, command: str) -> str:
        if command == "ping":
            return "pong"
        raise ValueError(f"unsupported command: {command}")

    def poll_push(self, timeout: float | None = 0.0, *, parse: bool = False) -> Any:
        with self._condition:
            push_buffer = self._push_buffer
        if push_buffer is None:
            return None
        item = push_buffer.poll(timeout)
        if item is None or not parse:
            return item.response if item is not None else None
        return _parse_push(item)

    def drain_pushes(self, *, parse: bool = False) -> list[Any]:
        with self._condition:
            push_buffer = self._push_buffer
        if push_buffer is None:
            return []
        items = push_buffer.drain()
        return [_parse_push(item) for item in items] if parse else [item.response for item in items]

    def _ensure_started(self) -> tuple[LeaseBroker, PushBuffer]:
        broker, push_buffer, _ = self._ensure_started_before(None)
        return broker, push_buffer

    def _observe_overlapping_dns_attempt(
        self,
        entered_at: float,
        waited_attempt: StartupAttempt | None,
    ) -> StartupAttempt | None:
        with self._startup_observer_lock:
            attempt = self._observable_startup_attempt
            if (
                attempt is None
                or not attempt.dns_preflight
                or attempt.dns_started_at is None
                or (waited_attempt is not None and attempt is not waited_attempt)
                or (attempt.dns_completed_at is not None and entered_at > attempt.dns_completed_at)
            ):
                return None
            return attempt

    def _observable_dns_deadline(self, attempt: StartupAttempt) -> float | None:
        with self._startup_observer_lock:
            if self._observable_startup_attempt is not attempt:
                return None
            return attempt.request_deadline

    def _complete_startup_dns_preflight(
        self,
        attempt: StartupAttempt,
        completed_at: float,
        request_deadline: float | None,
    ) -> None:
        with self._startup_observer_lock:
            attempt.dns_completed_at = completed_at
            attempt.request_deadline = request_deadline
        attempt.dns_ready.set()

    def _set_observable_startup_attempt(self, attempt: StartupAttempt | None) -> None:
        with self._startup_observer_lock:
            self._observable_startup_attempt = attempt

    def _ensure_started_before(
        self,
        deadline: float | None,
    ) -> tuple[LeaseBroker, PushBuffer, float | None]:
        entered_at = time.monotonic()
        waited_attempt: StartupAttempt | None = None
        while True:
            if deadline is None:
                self._condition.acquire()
                acquired = True
            else:
                acquired = self._condition.acquire(
                    timeout=max(0.0, deadline - time.monotonic())
                )
                if not acquired:
                    observed_attempt = self._observe_overlapping_dns_attempt(entered_at, waited_attempt)
                    if observed_attempt is not None:
                        if waited_attempt is None:
                            waited_attempt = observed_attempt
                        observed_attempt.dns_ready.wait()
                        shared_deadline = self._observable_dns_deadline(observed_attempt)
                        if shared_deadline is not None:
                            acquired = self._condition.acquire(
                                timeout=max(0.0, shared_deadline - time.monotonic())
                            )
            if not acquired:
                raise ResponseTimeoutError("7709 response timed out during pool startup admission")
            try:
                if self._startup_aborted.is_set():
                    self._startup_active = False
                    self._condition.notify_all()
                failure = None
                if self._state is PoolState.RUNNING and self._broker is not None and self._push_buffer is not None:
                    failure = self._runtime_guard.failure(deadline=deadline)
                if failure is not None:
                    self._state = PoolState.FAILED
                    raise ConnectionClosedError("7709 pool failed because an Actor terminated") from failure
                retire_requested = self._epoch_retire_event is not None and self._epoch_retire_event.is_set()
                if self._shutdown_failed.is_set() or retire_requested:
                    if self._shutdown_failed.is_set() and self._state is not PoolState.FAILED_CLOSED:
                        self._state = PoolState.FAILED_CLOSING
                    reported_state = self._state
                    if retire_requested and reported_state not in (
                        PoolState.CLOSING,
                        PoolState.FAILED,
                        PoolState.FAILED_CLOSING,
                        PoolState.FAILED_CLOSED,
                    ):
                        reported_state = PoolState.CLOSING
                    raise ConnectionClosedError(f"7709 pool is not usable: {reported_state.name}")
                if self._state is PoolState.RUNNING and self._broker is not None and self._push_buffer is not None:
                    published_attempt = self._published_startup_attempt
                    if (
                        published_attempt is not None
                        and published_attempt.request_deadline is not None
                        and (
                            waited_attempt is published_attempt
                            or (
                                published_attempt.dns_preflight
                                and published_attempt.dns_completed_at is not None
                                and waited_attempt is None
                                and entered_at
                                <= published_attempt.dns_completed_at
                            )
                        )
                    ):
                        deadline = published_attempt.request_deadline
                    return self._broker, self._push_buffer, deadline
                if self._state in (PoolState.CLOSING, PoolState.FAILED, PoolState.FAILED_CLOSING, PoolState.FAILED_CLOSED):
                    raise ConnectionClosedError(f"7709 pool is not usable: {self._state.name}")
                if self._startup_active:
                    active_attempt = self._startup_attempt
                    crosses_active_dns = (
                        active_attempt is not None
                        and active_attempt.dns_preflight
                        and active_attempt.dns_started_at is not None
                        and (
                            active_attempt.dns_completed_at is None
                            or entered_at <= active_attempt.dns_completed_at
                        )
                    )
                    if crosses_active_dns:
                        waited_attempt = active_attempt
                    if (
                        crosses_active_dns
                        and active_attempt is not None
                        and active_attempt.request_deadline is not None
                    ):
                        deadline = active_attempt.request_deadline
                    dns_wait = (
                        crosses_active_dns
                        and active_attempt is not None
                        and active_attempt.request_deadline is None
                    )
                    remaining = None if deadline is None or dns_wait else max(0.0, deadline - time.monotonic())
                    if remaining == 0 or not self._condition.wait(timeout=remaining):
                        raise ResponseTimeoutError("7709 response timed out during pool startup")
                    continue
                if deadline is not None and deadline <= time.monotonic():
                    raise ResponseTimeoutError("7709 response timed out during pool startup")
                self._startup_active = True
                self._startup_aborted.clear()
                self._startup_cleanup_error = None
                self._state = PoolState.STARTING
                observed_epoch = self._epoch
                candidate_epoch = observed_epoch + 1
                retire_event = threading.Event()
                self._epoch_retire_event = retire_event
                self._shutdown_failed = threading.Event()
                dns_preflight = _requires_dns(self._hosts)
                startup_attempt = StartupAttempt(
                    candidate_epoch,
                    retire_event,
                    dns_preflight=dns_preflight,
                    dns_started_at=time.monotonic() if dns_preflight else None,
                )
                self._startup_attempt = startup_attempt
                self._published_startup_attempt = None
                self._set_observable_startup_attempt(startup_attempt)
                waited_attempt = None
                break
            finally:
                self._condition.release()

        broker: LeaseBroker | None = None
        push_buffer: PushBuffer | None = None
        configured: list[tuple[SocketTransport, RuntimeRegistration]] = []
        try:
            dns_preflight = startup_attempt.dns_preflight
            try:
                endpoint_sets = [resolve_hosts(_rotate_hosts(self._hosts, index)) for index in range(self._pool_size)]
            except OSError as exc:
                if dns_preflight:
                    dns_completed_at = time.monotonic()
                    if deadline is not None:
                        deadline = dns_completed_at + self._timeout
                    self._complete_startup_dns_preflight(startup_attempt, dns_completed_at, None)
                raise ConnectionClosedError("7709 unable to resolve any configured host") from exc
            except BaseException:
                if dns_preflight:
                    self._complete_startup_dns_preflight(startup_attempt, time.monotonic(), None)
                raise
            if dns_preflight:
                dns_completed_at = time.monotonic()
                if deadline is not None:
                    deadline = dns_completed_at + self._timeout
                self._complete_startup_dns_preflight(startup_attempt, dns_completed_at, deadline)
            else:
                dns_completed_at = None
            if deadline is None:
                self._condition.acquire()
                acquired = True
            else:
                acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if not acquired:
                raise ResponseTimeoutError("7709 response timed out publishing pool endpoints")
            try:
                if dns_preflight:
                    self._condition.notify_all()
                if (
                    self._epoch != observed_epoch
                    or self._state is not PoolState.STARTING
                    or retire_event.is_set()
                    or self._shutdown_failed.is_set()
                ):
                    raise ConnectionClosedError("7709 pool changed while resolving endpoints")
            finally:
                self._condition.release()
            push_buffer = PushBuffer(
                candidate_epoch,
                max_frames=self._push_queue_size,
                max_bytes=self._push_queue_bytes,
                retire_event=retire_event,
            )
            broker = LeaseBroker(
                candidate_epoch,
                self._pool_size,
                self._max_pending_requests,
                retire_event=retire_event,
            )
            startup_attempt.broker = broker
            startup_attempt.push_buffer = push_buffer
            heartbeat_allowed = HeartbeatAdmissionGuard(broker)
            self._runtime_guard.configure(
                broker,
                push_buffer,
                retire_event=retire_event,
                deadline=deadline,
            )
            guard_ref = weakref.ref(self._runtime_guard)
            broker_ref = weakref.ref(broker)
            successor_grace, terminal_yield = _actor_cooperation(self._pool_size)
            for transport, endpoints in zip(self._transports, endpoint_sets):
                registration = RuntimeRegistration(guard_ref, candidate_epoch, broker_ref, retire_event)
                startup_attempt.configured.append((transport, registration))
                transport._configure_pool_runtime(
                    push_buffer=push_buffer,
                    runtime_epoch=candidate_epoch,
                    endpoints=endpoints,
                    actor_fatal_callback=ActorFatalHandle(
                        guard_ref,
                        candidate_epoch,
                        broker_ref,
                        retire_event,
                    ),
                    runtime_started_callback=registration,
                    heartbeat_allowed=heartbeat_allowed,
                    successor_grace=successor_grace,
                    terminal_yield=terminal_yield,
                    deadline=deadline,
                )
                configured.append((transport, registration))
        except BaseException as startup_error:
            retire_event.set()
            cleanup_errors = self._cleanup_unpublished_pool_epoch(
                broker=broker,
                push_buffer=push_buffer,
                runtime_epoch=candidate_epoch,
                configured=startup_attempt.configured,
                deadline=deadline,
            )
            startup_attempt.cleanup_complete = not cleanup_errors
            startup_attempt.error = cleanup_errors[0] if cleanup_errors else startup_error
            startup_attempt.completed.set()
            if deadline is None:
                self._condition.acquire()
                acquired = True
            else:
                acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if not acquired:
                self._shutdown_failed.set()
                retire_event.set()
                self._startup_aborted.set()
                raise
            try:
                if cleanup_errors or self._shutdown_failed.is_set():
                    self._broker = broker
                    self._push_buffer = push_buffer
                    self._registrations = tuple(registration for _, registration in configured)
                    self._startup_cleanup_error = cleanup_errors[0] if cleanup_errors else ConnectionClosedError(
                        "7709 pool shutdown failed during startup"
                    )
                    self._epoch = max(self._epoch, candidate_epoch)
                    self._state = PoolState.FAILED_CLOSING
                elif self._state is PoolState.STARTING:
                    self._state = PoolState.STOPPED
                    if self._epoch_retire_event is retire_event:
                        self._epoch_retire_event = None
                    if self._startup_attempt is startup_attempt and startup_attempt.cleanup_complete:
                        self._startup_attempt = None
                self._startup_active = False
                self._condition.notify_all()
            finally:
                self._condition.release()
            raise

        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            self._shutdown_failed.set()
            retire_event.set()
            self._startup_aborted.set()
            publish = False
        else:
            try:
                if (
                    self._epoch != observed_epoch
                    or self._state is not PoolState.STARTING
                    or retire_event.is_set()
                    or self._shutdown_failed.is_set()
                ):
                    publish = False
                else:
                    self._epoch = candidate_epoch
                    self._broker = broker
                    self._push_buffer = push_buffer
                    self._registrations = tuple(registration for _, registration in configured)
                    self._state = PoolState.RUNNING
                    publish = True
                if publish:
                    self._startup_active = False
                    self._published_startup_attempt = startup_attempt
                    startup_attempt.completed.set()
                    if self._startup_attempt is startup_attempt:
                        self._startup_attempt = None
                    self._condition.notify_all()
            finally:
                self._condition.release()
        if not publish:
            retire_event.set()
            cleanup_errors: list[BaseException] = []
            try:
                cleanup_errors = self._cleanup_unpublished_pool_epoch(
                    broker=broker,
                    push_buffer=push_buffer,
                    runtime_epoch=candidate_epoch,
                    configured=startup_attempt.configured,
                    deadline=deadline,
                )
                startup_attempt.cleanup_complete = not cleanup_errors
                startup_attempt.error = cleanup_errors[0] if cleanup_errors else ConnectionClosedError(
                    "7709 pool changed during startup"
                )
                startup_attempt.completed.set()
            finally:
                if deadline is None:
                    self._condition.acquire()
                    acquired = True
                else:
                    acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
                if not acquired:
                    self._shutdown_failed.set()
                    retire_event.set()
                    self._startup_aborted.set()
                else:
                    try:
                        if cleanup_errors or self._shutdown_failed.is_set():
                            self._broker = broker
                            self._push_buffer = push_buffer
                            self._registrations = tuple(registration for _, registration in configured)
                            self._startup_cleanup_error = cleanup_errors[0] if cleanup_errors else ConnectionClosedError(
                                "7709 pool shutdown failed during startup"
                            )
                            self._epoch = max(self._epoch, candidate_epoch)
                            self._state = PoolState.FAILED_CLOSING
                        self._startup_active = False
                        if self._startup_attempt is startup_attempt and startup_attempt.cleanup_complete:
                            self._startup_attempt = None
                        self._condition.notify_all()
                    finally:
                        self._condition.release()
            if cleanup_errors:
                raise cleanup_errors[0]
            raise ConnectionClosedError("7709 pool changed while resolving endpoints")
        return broker, push_buffer, deadline

    def _cleanup_unpublished_pool_epoch(
        self,
        *,
        broker: LeaseBroker | None,
        push_buffer: PushBuffer | None,
        runtime_epoch: int,
        configured: Sequence[tuple[SocketTransport, RuntimeRegistration]],
        deadline: float | None = None,
    ) -> list[BaseException]:
        errors: list[BaseException] = []
        for transport, registration in configured:
            try:
                transport._retire_pool_runtime(registration, deadline=deadline)
            except BaseException as exc:
                errors.append(exc)
        if broker is not None:
            try:
                self._runtime_guard.seal(pool_epoch=runtime_epoch, broker=broker, deadline=deadline)
            except BaseException as exc:
                errors.append(exc)
            try:
                broker.close(deadline=deadline)
            except BaseException as exc:
                errors.append(exc)
        if push_buffer is not None:
            try:
                if deadline is None:
                    push_buffer.close()
                else:
                    push_buffer.close_before_deadline(deadline)
            except BaseException as exc:
                errors.append(exc)

        all_cleared = broker is not None and push_buffer is not None
        if broker is not None and push_buffer is not None:
            for transport, registration in configured:
                try:
                    cleared = transport._clear_pool_runtime(
                        registration=registration,
                        runtime_epoch=runtime_epoch,
                        push_buffer=push_buffer,
                        deadline=deadline,
                    )
                    if not cleared:
                        all_cleared = False
                        errors.append(RuntimeError("7709 pool startup slot configuration was not cleared"))
                except BaseException as exc:
                    all_cleared = False
                    errors.append(exc)
        if all_cleared and broker is not None:
            try:
                finish_error = self._runtime_guard.finish_epoch(
                    pool_epoch=runtime_epoch,
                    broker=broker,
                    deadline=deadline,
                )
                if finish_error is not None:
                    errors.append(finish_error)
            except BaseException as exc:
                errors.append(exc)
        return errors

    def _begin_connect_rollback(
        self,
        broker: LeaseBroker,
        push_buffer: PushBuffer,
        *,
        deadline: float | None = None,
    ) -> None:
        del broker, push_buffer, deadline
        self._runtime_guard.request_seal()

    def _shutdown(self, *, normal: bool, already_requested_stop: bool = False) -> None:
        del normal
        caller_deadline = time.monotonic() + 1.0
        retire_event = self._epoch_retire_event
        if retire_event is not None:
            retire_event.set()
        self._runtime_guard.request_seal()
        try:
            attempt, owner = self._claim_shutdown_attempt(deadline=caller_deadline)
        except BaseException:
            self._shutdown_failed.set()
            _request_stop_transports(tuple(self._transports), caller_deadline)
            raise
        if attempt is None:
            return
        if not owner:
            self._wait_shutdown_attempt(attempt, deadline=caller_deadline)
            return
        self._run_shutdown_attempt(attempt, already_requested_stop=already_requested_stop)

    def _claim_shutdown_attempt(
        self,
        *,
        expected_broker: LeaseBroker | None = None,
        expected_push_buffer: PushBuffer | None = None,
        deadline: float | None = None,
    ) -> tuple[ShutdownAttempt | None, bool]:
        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            if expected_broker is None:
                retire_event = self._epoch_retire_event
                if retire_event is not None:
                    retire_event.set()
                self._runtime_guard.request_seal()
                self._shutdown_failed.set()
            raise TransportCloseTimeoutError("7709 pool condition blocked shutdown claim before deadline")
        try:
            current_attempt = self._shutdown_attempt
            if current_attempt is not None and not current_attempt.completed.is_set():
                if expected_broker is not None and (
                    current_attempt.broker is not expected_broker
                    or current_attempt.push_buffer is not expected_push_buffer
                ):
                    return None, False
                return current_attempt, False
            if expected_broker is not None and (
                self._broker is not expected_broker
                or self._push_buffer is not expected_push_buffer
                or self._state is not PoolState.RUNNING
            ):
                return None, False
            if self._state is PoolState.FAILED_CLOSED:
                return None, False
            if self._state is PoolState.STOPPED and not self._shutdown_failed.is_set():
                return None, False
            retire_event = self._epoch_retire_event
            if retire_event is not None:
                retire_event.set()
            self._runtime_guard.request_seal()
            self._shutdown_generation += 1
            attempt = ShutdownAttempt(
                generation=self._shutdown_generation,
                deadline=time.monotonic() + 1.0 if deadline is None else deadline,
                broker=self._broker,
                push_buffer=self._push_buffer,
                connect_futures=self._connect_futures,
                connect_executor=self._connect_executor,
            )
            if self._shutdown_failed.is_set() or self._state in (PoolState.STOPPED, PoolState.FAILED):
                self._state = PoolState.FAILED_CLOSING
            elif self._state not in (PoolState.CLOSING, PoolState.FAILED_CLOSING):
                self._state = PoolState.CLOSING
                self._epoch += 1
            self._shutdown_attempt = attempt
            self._shutdown_active = True
            self._condition.notify_all()
            return attempt, True
        finally:
            self._condition.release()

    def _wait_shutdown_attempt(self, attempt: ShutdownAttempt, *, deadline: float | None = None) -> None:
        wait_deadline = attempt.deadline if deadline is None else min(attempt.deadline, deadline)
        if not attempt.completed.wait(max(0.0, wait_deadline - time.monotonic())):
            error = TransportCloseTimeoutError("7709 pool shutdown did not finish before close deadline")
            acquired = self._condition.acquire(timeout=max(0.0, wait_deadline - time.monotonic()))
            if acquired:
                try:
                    if not attempt.completed.is_set():
                        if attempt.forced_error is None:
                            attempt.forced_error = error
                        self._shutdown_failed.set()
                        self._state = PoolState.FAILED_CLOSING
                        self._condition.notify_all()
                    else:
                        error = attempt.error or error
                finally:
                    self._condition.release()
            elif not attempt.completed.is_set():
                if attempt.forced_error is None:
                    attempt.forced_error = error
                self._shutdown_failed.set()
            else:
                error = attempt.error or error
            raise error
        if attempt.error is not None:
            raise attempt.error

    def _run_shutdown_attempt(
        self,
        attempt: ShutdownAttempt,
        *,
        already_requested_stop: bool,
        initial_errors: Sequence[BaseException] = (),
    ) -> None:

        deadline = attempt.deadline
        errors = list(initial_errors)
        failed_before_close = False
        broker: LeaseBroker | None = None
        push_buffer: PushBuffer | None = None
        registrations: tuple[RuntimeRegistration, ...] = ()
        startup_attempt: StartupAttempt | None = None
        transports = tuple(self._transports)
        normal_cleanup = False
        pool_configuration_cleared = False
        failed_closed = False
        self._runtime_guard.request_seal()
        if not already_requested_stop:
            errors.extend(_request_stop_transports(transports, deadline))

        try:
            try:
                guard_failure = self._runtime_guard.failure(deadline=deadline)
            except BaseException as exc:
                errors.append(exc)
                guard_failure = exc
            try:
                guard_cleanup_failure = self._runtime_guard.cleanup_failure(deadline=deadline)
            except BaseException as exc:
                errors.append(exc)
                guard_cleanup_failure = exc
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if not acquired:
                raise TransportCloseTimeoutError("7709 pool condition blocked shutdown before deadline")
            try:
                if self._startup_aborted.is_set():
                    self._startup_active = False
                    self._condition.notify_all()
                startup_attempt = self._startup_attempt
                already_retired = self._state in (PoolState.CLOSING, PoolState.FAILED_CLOSING)
                failed_before_close = self._shutdown_failed.is_set() or guard_failure is not None or self._state in (
                    PoolState.FAILED,
                    PoolState.FAILED_CLOSING,
                )
                if failed_before_close:
                    self._state = PoolState.FAILED_CLOSING
                elif self._state is not PoolState.CLOSING:
                    self._state = PoolState.CLOSING
                if not already_retired:
                    self._epoch += 1
                self._condition.notify_all()
                while self._startup_active:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not self._condition.wait(timeout=remaining):
                        errors.append(TransportCloseTimeoutError("7709 pool startup did not finish before close deadline"))
                        break
                if not self._startup_active:
                    if self._startup_cleanup_error is not None:
                        errors.append(self._startup_cleanup_error)
                        self._startup_cleanup_error = None
                    failed_before_close = failed_before_close or self._state in (
                        PoolState.FAILED,
                        PoolState.FAILED_CLOSING,
                    )
                    if failed_before_close:
                        self._state = PoolState.FAILED_CLOSING
                broker = self._broker or (startup_attempt.broker if startup_attempt is not None else None)
                push_buffer = self._push_buffer or (
                    startup_attempt.push_buffer if startup_attempt is not None else None
                )
                registrations = (
                    tuple(registration for _, registration in startup_attempt.configured)
                    if startup_attempt is not None
                    else self._registrations
                )
            finally:
                self._condition.release()

            cleanup_error_count = len(errors)
            if broker is not None:
                try:
                    self._runtime_guard.seal(
                        pool_epoch=broker.pool_epoch,
                        broker=broker,
                        deadline=deadline,
                    )
                except BaseException as exc:
                    errors.append(exc)
                try:
                    broker.close(deadline=deadline)
                except BaseException as exc:
                    errors.append(exc)
            if push_buffer is not None:
                try:
                    push_buffer.close_before_deadline(deadline)
                except BaseException as exc:
                    errors.append(exc)

            if attempt.connect_futures:
                _, pending_connect = wait(
                    attempt.connect_futures,
                    timeout=max(0.0, deadline - time.monotonic()),
                    return_when=ALL_COMPLETED,
                )
                if pending_connect:
                    errors.append(
                        TransportCloseTimeoutError("7709 pool connect workers did not finish before close deadline")
                    )
                if attempt.connect_executor is not None:
                    try:
                        attempt.connect_executor.shutdown(wait=False, cancel_futures=True)
                        executor_threads = _executor_worker_threads(attempt.connect_executor)
                        for thread in executor_threads:
                            thread.join(timeout=max(0.0, deadline - time.monotonic()))
                        if any(thread.is_alive() for thread in executor_threads):
                            errors.append(
                                TransportCloseTimeoutError(
                                    "7709 pool connect executor did not stop before close deadline"
                                )
                            )
                    except BaseException as exc:
                        errors.append(exc)
            for transport, registration in zip(transports, registrations):
                try:
                    transport._retire_pool_runtime(registration, deadline=deadline)
                except BaseException as exc:
                    errors.append(exc)
            for transport in transports:
                try:
                    transport._close_with_timeout(max(0.0, deadline - time.monotonic()))
                except BaseException as exc:
                    errors.append(exc)

            if (
                guard_cleanup_failure is not None
                and broker is not None
                and len(errors) == cleanup_error_count
            ):
                try:
                    self._runtime_guard.clear_cleanup_failure(
                        pool_epoch=broker.pool_epoch,
                        broker=broker,
                        deadline=deadline,
                    )
                except BaseException as exc:
                    errors.append(exc)

            try:
                late_failure = self._runtime_guard.failure(deadline=deadline)
            except BaseException as exc:
                errors.append(exc)
                late_failure = exc
            try:
                late_cleanup_failure = self._runtime_guard.cleanup_failure(deadline=deadline)
            except BaseException as exc:
                errors.append(exc)
                late_cleanup_failure = exc
            if late_cleanup_failure is not None:
                errors.append(late_cleanup_failure)
            failed_closed = failed_before_close or late_failure is not None
            if not errors:
                cleared = True
                if broker is not None and push_buffer is not None:
                    for transport, registration in zip(transports, registrations):
                        try:
                            cleared = transport._clear_pool_runtime(
                                registration=registration,
                                runtime_epoch=broker.pool_epoch,
                                push_buffer=push_buffer,
                                deadline=deadline,
                            ) and cleared
                        except BaseException as exc:
                            errors.append(exc)
                            cleared = False
                    if cleared and not errors:
                        finish_error = self._runtime_guard.finish_epoch(
                            pool_epoch=broker.pool_epoch,
                            broker=broker,
                            deadline=deadline,
                        )
                        if finish_error is not None:
                            failed_closed = True
                if cleared and not errors and not failed_closed:
                    normal_cleanup = True
                if cleared and not errors:
                    pool_configuration_cleared = True
        except BaseException as exc:
            errors.append(exc)
        finally:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if acquired:
                try:
                    executor_terminal = (
                        attempt.connect_executor is not None
                        and self._connect_executor is attempt.connect_executor
                        and all(future.done() for future in attempt.connect_futures)
                        and not any(
                            thread.is_alive()
                            for thread in _executor_worker_threads(attempt.connect_executor)
                        )
                    )
                    if executor_terminal:
                        self._connect_executor = None
                        self._connect_futures = ()
                        self._connect_shutdown_started = False
                    if self._connect_broker is broker:
                        self._connect_broker = None
                    if attempt.forced_error is not None:
                        errors.append(attempt.forced_error)
                    if errors:
                        self._shutdown_failed.set()
                        self._state = PoolState.FAILED_CLOSING
                    elif failed_closed:
                        self._state = PoolState.FAILED_CLOSED
                        if pool_configuration_cleared and self._startup_attempt is startup_attempt:
                            self._startup_attempt = None
                        if pool_configuration_cleared:
                            self._published_startup_attempt = None
                            self._set_observable_startup_attempt(None)
                    elif normal_cleanup:
                        self._state = PoolState.STOPPED
                        self._broker = None
                        self._push_buffer = None
                        self._registrations = ()
                        self._epoch_retire_event = None
                        self._shutdown_failed = threading.Event()
                        if self._startup_attempt is startup_attempt:
                            self._startup_attempt = None
                        self._published_startup_attempt = None
                        self._set_observable_startup_attempt(None)
                    else:
                        self._state = PoolState.FAILED_CLOSING
                        errors.append(TransportCloseTimeoutError("7709 pool shutdown did not reach a terminal state"))
                    attempt.error = errors[0] if errors else None
                    attempt.broker = None
                    attempt.push_buffer = None
                    self._shutdown_active = False
                    attempt.completed.set()
                    self._condition.notify_all()
                finally:
                    self._condition.release()
            else:
                errors.append(TransportCloseTimeoutError("7709 pool condition blocked shutdown publication"))
                if attempt.forced_error is not None:
                    errors.append(attempt.forced_error)
                self._shutdown_failed.set()
                attempt.error = errors[0] if errors else None
                attempt.broker = None
                attempt.push_buffer = None
                attempt.completed.set()

        if attempt.error is not None:
            raise attempt.error

    def _new_transport(self, index: int) -> SocketTransport:
        return SocketTransport(
            hosts=_rotate_hosts(self._hosts, index),
            timeout=self._timeout,
            heartbeat_interval=self._heartbeat_interval,
            push_queue_size=self._push_queue_size,
            push_queue_bytes=self._push_queue_bytes,
        )


def _rotate_hosts(hosts: list[str], offset: int) -> list[str]:
    if not hosts:
        return []
    index = offset % len(hosts)
    return hosts[index:] + hosts[:index]


def _actor_cooperation(pool_size: int) -> tuple[float, bool]:
    if sys.platform != "win32":
        return 0.0, False
    if pool_size == 1:
        return 0.0, True
    return 0.002, False


def _executor_worker_threads(executor: Any) -> tuple[threading.Thread, ...]:
    threads: list[threading.Thread] = []
    seen: set[int] = set()
    current: Any = executor
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for thread in getattr(current, "_threads", ()):
            if all(existing is not thread for existing in threads):
                threads.append(thread)
        current = getattr(current, "inner", None)
    return tuple(threads)


def _request_stop_transports(
    transports: Sequence[SocketTransport],
    deadline: float,
) -> list[BaseException]:
    retry: list[SocketTransport] = []
    errors: list[BaseException] = []
    for transport in transports:
        try:
            transport._request_stop(deadline=time.monotonic())
        except TransportCloseTimeoutError:
            retry.append(transport)
        except BaseException as exc:
            errors.append(exc)

    for index, transport in enumerate(retry):
        remaining_count = len(retry) - index
        now = time.monotonic()
        fair_deadline = now + max(0.0, deadline - now) / remaining_count
        try:
            transport._request_stop(deadline=fair_deadline)
        except BaseException as exc:
            errors.append(exc)
    return errors
