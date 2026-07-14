"""Bounded FIFO connection pool for the 7709 Actor transport."""

from __future__ import annotations

import threading
import time
import weakref
from collections import deque
from collections.abc import Sequence
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
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
from .actor import ActorRuntime, ActorSnapshot, actor_snapshot, request_actor_stop
from .socket import (
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_PUSH_QUEUE_BYTES,
    DEFAULT_PUSH_QUEUE_SIZE,
    SocketTransport,
    _parse_push,
)

DEFAULT_MAX_PENDING_REQUESTS = 256


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
    state: LeaseState = LeaseState.ACTIVE


@dataclass(slots=True)
class AdmissionWaiter:
    pool_epoch: int
    waiter_id: int
    deadline: float
    pinned: bool
    state: AdmissionState = AdmissionState.WAITING
    assigned_lease: SlotLease | None = None
    error: BaseException | None = None
    completed: threading.Event = field(default_factory=threading.Event)


@dataclass(frozen=True, slots=True)
class BrokerSnapshot:
    pool_epoch: int
    idle_slots: int
    waiter_count: int
    pin_waiter_count: int
    active_leases: int
    closed: bool


class LeaseBroker:
    """Pool scheduling core with no references to facades or Actor runtimes."""

    def __init__(self, pool_epoch: int, pool_size: int, max_pending_requests: int) -> None:
        self.pool_epoch = pool_epoch
        self.max_pending_requests = max_pending_requests
        self._condition = threading.Condition()
        self._idle_slots = deque(range(pool_size))
        self._waiters: deque[AdmissionWaiter] = deque()
        self._active_leases: dict[int, SlotLease] = {}
        self._waiter_counter = 0
        self._lease_counter = 0
        self._pin_waiters = 0
        self._pin_waiter_events: set[Any] = set()
        self._closed = False

    def acquire(self, deadline: float, *, pinned: bool = False) -> SlotLease:
        with self._condition:
            if self._closed:
                raise ConnectionClosedError("7709 pool is closed")
            if time.monotonic() >= deadline:
                raise ResponseTimeoutError("7709 response timed out during queue")
            if self._idle_slots and not self._waiters:
                return self._new_lease_locked(self._idle_slots.popleft(), pinned)
            if len(self._waiters) + self._pin_waiters >= self.max_pending_requests:
                raise PoolBusyError("7709 pool admission queue is full")
            self._waiter_counter += 1
            completed = threading.Event()
            waiter = AdmissionWaiter(self.pool_epoch, self._waiter_counter, deadline, pinned, completed=completed)
            self._waiters.append(waiter)
            self._condition.notify_all()
        remaining = max(0.0, deadline - time.monotonic())
        if not waiter.completed.wait(remaining):
            with self._condition:
                if waiter.state is AdmissionState.WAITING:
                    try:
                        self._waiters.remove(waiter)
                    except ValueError:
                        pass
                    waiter.state = AdmissionState.TIMED_OUT
                    waiter.error = ResponseTimeoutError("7709 response timed out during queue")
        with self._condition:
            if waiter.state is AdmissionState.ASSIGNED and waiter.assigned_lease is not None:
                lease = waiter.assigned_lease
                valid = (
                    not self._closed
                    and lease.state is LeaseState.ACTIVE
                    and self._active_leases.get(lease.lease_id) is lease
                )
                if valid:
                    return lease
                waiter.state = AdmissionState.CLOSED
                waiter.error = ConnectionClosedError("7709 pool closed during admission")
            error = waiter.error
        if error is not None:
            raise error
        raise ResponseTimeoutError("7709 response timed out during queue")

    def release(self, lease: SlotLease) -> bool:
        wake: list[AdmissionWaiter] = []
        with self._condition:
            current = self._active_leases.get(lease.lease_id)
            if current is not lease or lease.state is LeaseState.RELEASED:
                return False
            lease.state = LeaseState.RELEASED
            del self._active_leases[lease.lease_id]
            if self._closed:
                return True
            while self._waiters:
                waiter = self._waiters.popleft()
                if waiter.state is not AdmissionState.WAITING or waiter.pool_epoch != self.pool_epoch:
                    continue
                if time.monotonic() >= waiter.deadline:
                    waiter.state = AdmissionState.TIMED_OUT
                    waiter.error = ResponseTimeoutError("7709 response timed out during queue")
                    wake.append(waiter)
                    continue
                waiter.assigned_lease = self._new_lease_locked(lease.slot_id, waiter.pinned)
                waiter.state = AdmissionState.ASSIGNED
                wake.append(waiter)
                break
            else:
                self._idle_slots.append(lease.slot_id)
        for waiter in wake:
            waiter.completed.set()
        return True

    def validate(self, lease: SlotLease) -> bool:
        with self._condition:
            return (
                not self._closed
                and lease.pool_epoch == self.pool_epoch
                and lease.state is LeaseState.ACTIVE
                and self._active_leases.get(lease.lease_id) is lease
            )

    def reserve_pin_waiter(self, completed: Any = None) -> None:
        with self._condition:
            if self._closed:
                raise ConnectionClosedError("7709 pool is closed")
            if len(self._waiters) + self._pin_waiters >= self.max_pending_requests:
                raise PoolBusyError("7709 pool admission queue is full")
            self._pin_waiters += 1
            if completed is not None:
                self._pin_waiter_events.add(completed)
            self._condition.notify_all()

    def release_pin_waiter(self, completed: Any = None) -> None:
        with self._condition:
            if completed is not None:
                if completed not in self._pin_waiter_events:
                    return
                self._pin_waiter_events.remove(completed)
            if self._pin_waiters > 0:
                self._pin_waiters -= 1

    def close(self) -> None:
        wake: list[AdmissionWaiter] = []
        pin_wake: tuple[Any, ...] = ()
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._idle_slots.clear()
            for waiter in self._waiters:
                if waiter.state is AdmissionState.WAITING:
                    waiter.state = AdmissionState.CLOSED
                    waiter.error = ConnectionClosedError("7709 pool closed during admission")
                    wake.append(waiter)
            self._waiters.clear()
            for lease in self._active_leases.values():
                lease.state = LeaseState.RELEASED
            self._active_leases.clear()
            pin_wake = tuple(self._pin_waiter_events)
            self._pin_waiter_events.clear()
            self._pin_waiters = 0
        for waiter in wake:
            waiter.completed.set()
        for completed in pin_wake:
            completed.set()

    def snapshot(self) -> BrokerSnapshot:
        with self._condition:
            return BrokerSnapshot(
                pool_epoch=self.pool_epoch,
                idle_slots=len(self._idle_slots),
                waiter_count=sum(waiter.state is AdmissionState.WAITING for waiter in self._waiters),
                pin_waiter_count=self._pin_waiters,
                active_leases=len(self._active_leases),
                closed=self._closed,
            )

    def allows_heartbeat(self) -> bool:
        with self._condition:
            if self._closed or self._pin_waiters:
                return False
            if any(waiter.state is AdmissionState.WAITING for waiter in self._waiters):
                return False
            return not any(
                lease.state is LeaseState.ACTIVE and not lease.pinned
                for lease in self._active_leases.values()
            )

    def wait_for_waiters(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(
                lambda: sum(waiter.state is AdmissionState.WAITING for waiter in self._waiters) >= count,
                timeout=timeout,
            )

    def wait_for_pin_waiters(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(lambda: self._pin_waiters >= count, timeout=timeout)

    def _new_lease_locked(self, slot_id: int, pinned: bool) -> SlotLease:
        self._lease_counter += 1
        lease = SlotLease(self.pool_epoch, self._lease_counter, slot_id, pinned)
        self._active_leases[lease.lease_id] = lease
        return lease


class LeaseCompletion:
    def __init__(self, broker: LeaseBroker, lease: SlotLease) -> None:
        self._broker_ref = weakref.ref(broker)
        self._lease = lease
        self._released = False

    def __call__(self, ticket: object | None) -> None:
        if self._released:
            return
        self._released = True
        broker = self._broker_ref()
        if broker is not None:
            broker.release(self._lease)


class HeartbeatAdmissionGuard:
    """Allow pooled heartbeats without retaining the pool scheduling core."""

    def __init__(self, broker: LeaseBroker) -> None:
        self._broker_ref = weakref.ref(broker)

    def __call__(self) -> bool:
        broker = self._broker_ref()
        return broker is not None and broker.allows_heartbeat()


class PoolRuntimeGuard:
    """Facade-independent runtime group used by fatal paths and finalization."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._broker: LeaseBroker | None = None
        self._push_buffer: PushBuffer | None = None
        self._runtimes: list[ActorRuntime] = []
        self._fatal_error: BaseException | None = None
        self._epoch: int | None = None
        self._state = GuardState.INACTIVE

    def configure(self, broker: LeaseBroker, push_buffer: PushBuffer) -> None:
        with self._lock:
            self._broker = broker
            self._push_buffer = push_buffer
            self._runtimes = []
            self._fatal_error = None
            self._epoch = broker.pool_epoch
            self._state = GuardState.ACTIVE

    def is_active(self, *, pool_epoch: int, broker: LeaseBroker | None) -> bool:
        with self._lock:
            return (
                self._state is GuardState.ACTIVE
                and self._fatal_error is None
                and broker is not None
                and broker is self._broker
                and pool_epoch == self._epoch
            )

    def add_runtime(
        self,
        runtime: ActorRuntime,
        *,
        pool_epoch: int,
        broker: LeaseBroker | None = None,
    ) -> bool:
        with self._lock:
            accepted = (
                self._state is GuardState.ACTIVE
                and self._fatal_error is None
                and self._broker is not None
                and runtime.runtime_epoch == self._epoch
                and pool_epoch == self._epoch
                and broker is self._broker
            )
            if accepted and all(item is not runtime for item in self._runtimes):
                self._runtimes.append(runtime)
        if not accepted:
            request_actor_stop(runtime)
        return accepted

    def fail(
        self,
        runtime: ActorRuntime,
        error: BaseException,
        *,
        pool_epoch: int,
        broker: LeaseBroker | None = None,
    ) -> None:
        with self._lock:
            accepted = (
                self._state in (GuardState.ACTIVE, GuardState.SEALED)
                and self._broker is not None
                and runtime.runtime_epoch == self._epoch
                and pool_epoch == self._epoch
                and broker is self._broker
            )
            if accepted:
                if self._fatal_error is None:
                    self._fatal_error = error
                if all(item is not runtime for item in self._runtimes):
                    self._runtimes.append(runtime)
                current_broker = self._broker
                push_buffer = self._push_buffer
                runtimes = tuple(self._runtimes)
            else:
                current_broker = None
                push_buffer = None
                runtimes = (runtime,)
        if current_broker is not None:
            current_broker.close()
        if push_buffer is not None:
            push_buffer.close(error)
        for item in runtimes:
            request_actor_stop(item)

    def seal(self, *, pool_epoch: int, broker: LeaseBroker | None) -> bool:
        with self._lock:
            if (
                self._state not in (GuardState.ACTIVE, GuardState.SEALED)
                or broker is None
                or broker is not self._broker
                or pool_epoch != self._epoch
            ):
                return False
            self._state = GuardState.SEALED
            return True

    def abandon(self) -> None:
        with self._lock:
            if self._state is GuardState.ACTIVE:
                self._state = GuardState.SEALED
            broker = self._broker
            push_buffer = self._push_buffer
            runtimes = tuple(self._runtimes)
        if broker is not None:
            broker.close()
        if push_buffer is not None:
            push_buffer.close()
        for runtime in runtimes:
            request_actor_stop(runtime)

    def failure(self) -> BaseException | None:
        with self._lock:
            return self._fatal_error

    def finish_epoch(self, *, pool_epoch: int, broker: LeaseBroker) -> BaseException | None:
        with self._lock:
            if broker is not self._broker or pool_epoch != self._epoch:
                return self._fatal_error
            error = self._fatal_error
            if error is not None:
                self._state = GuardState.SEALED
                return error
            self._broker = None
            self._push_buffer = None
            self._runtimes = []
            self._fatal_error = None
            self._epoch = None
            self._state = GuardState.INACTIVE
            return None


@dataclass(frozen=True, slots=True)
class ActorFatalHandle:
    guard_ref: weakref.ReferenceType[PoolRuntimeGuard]
    pool_epoch: int
    broker_ref: weakref.ReferenceType[LeaseBroker]

    def __call__(self, runtime: ActorRuntime, error: BaseException) -> None:
        guard = self.guard_ref()
        if guard is None:
            request_actor_stop(runtime)
            return
        guard.fail(runtime, error, pool_epoch=self.pool_epoch, broker=self.broker_ref())


@dataclass(frozen=True, slots=True)
class RuntimeRegistration:
    guard_ref: weakref.ReferenceType[PoolRuntimeGuard]
    pool_epoch: int
    broker_ref: weakref.ReferenceType[LeaseBroker]

    def __call__(self, runtime: ActorRuntime) -> bool:
        guard = self.guard_ref()
        if guard is None:
            request_actor_stop(runtime)
            return False
        return guard.add_runtime(runtime, pool_epoch=self.pool_epoch, broker=self.broker_ref())

    def is_active(self) -> bool:
        guard = self.guard_ref()
        return guard is not None and guard.is_active(pool_epoch=self.pool_epoch, broker=self.broker_ref())


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
    broker: LeaseBroker | None = None
    push_buffer: PushBuffer | None = None
    completed: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None


@dataclass(slots=True)
class PinWaiter:
    call_id: int
    deadline: float
    completed: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    reserved: bool = False
    assigned: bool = False


class PinCompletion:
    def __init__(self, proxy: PinnedTransportProxy, call_id: int) -> None:
        self._proxy: PinnedTransportProxy | None = proxy
        self._call_id = call_id
        self._lock = threading.Lock()
        self._done = False

    def __call__(self, ticket: object | None) -> None:
        with self._lock:
            if self._done:
                return
            self._done = True
        proxy = self._proxy
        try:
            if proxy is not None:
                proxy._wire_terminal(self._call_id)
        finally:
            self._proxy = None


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
        self._call_counter = 0
        self._active_call: int | None = None
        self._state = PinState.OPEN

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
        completion = PinCompletion(self, call_id)
        runtime = self._slot._runtime
        self._slot._connect_with_deadline(
            deadline=deadline,
            completion=completion,
            runtime=runtime,
            lock_slot=False,
            lease_id=self._lease.lease_id,
            expected_runtime_epoch=self._lease.pool_epoch,
        )

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        deadline = time.monotonic() + self._timeout
        call_id = self._admit(deadline)
        completion = PinCompletion(self, call_id)
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
        with self._condition:
            while self._state is PinState.CLOSING:
                self._condition.wait()
            if self._state is PinState.CLOSED:
                return
            self._state = PinState.CLOSING
            while self._waiters:
                waiter = self._waiters.popleft()
                waiter.error = ConnectionClosedError("pinned transport closed")
                if waiter.reserved:
                    waiter.reserved = False
                    self._broker.release_pin_waiter(waiter.completed)
                waiter.completed.set()
            active = self._active_call
        try:
            if active is not None:
                self._slot._cancel_lease(self._lease.lease_id)
                deadline = time.monotonic() + min(1.0, self._timeout)
                with self._condition:
                    if not self._condition.wait_for(
                        lambda: self._active_call is None,
                        timeout=max(0.0, deadline - time.monotonic()),
                    ):
                        raise TransportCloseTimeoutError("pinned transport did not quiesce")
            self._broker.release(self._lease)
        except BaseException:
            release_failed_lease = False
            with self._condition:
                self._state = PinState.FAILED
                release_failed_lease = self._active_call is None
                self._condition.notify_all()
            if release_failed_lease:
                self._release_failed_lease()
            raise
        with self._condition:
            self._state = PinState.CLOSED
            self._condition.notify_all()

    def _validate(self) -> None:
        with self._condition:
            open_state = self._state is PinState.OPEN
        if not open_state or not self._broker.validate(self._lease):
            raise ConnectionClosedError("pinned transport lease is no longer valid")

    def _admit(self, deadline: float) -> int:
        with self._condition:
            if self._state is not PinState.OPEN or not self._broker.validate(self._lease):
                raise ConnectionClosedError("pinned transport lease is no longer valid")
            if time.monotonic() >= deadline:
                raise ResponseTimeoutError("7709 response timed out during queue")
            self._call_counter += 1
            call_id = self._call_counter
            if self._active_call is None and not self._waiters:
                self._active_call = call_id
                return call_id
            waiter = PinWaiter(call_id, deadline)
            self._broker.reserve_pin_waiter(waiter.completed)
            waiter.reserved = True
            self._waiters.append(waiter)
        if not waiter.completed.wait(max(0.0, deadline - time.monotonic())):
            with self._condition:
                if waiter.assigned:
                    pass
                elif not waiter.completed.is_set():
                    try:
                        self._waiters.remove(waiter)
                    except ValueError:
                        pass
                    waiter.error = ResponseTimeoutError("7709 response timed out during queue")
            if waiter.assigned:
                waiter.completed.wait()
            if waiter.reserved:
                self._broker.release_pin_waiter(waiter.completed)
                waiter.reserved = False
        release_failed_lease = False
        with self._condition:
            if waiter.error is None:
                lease_valid = self._broker.validate(self._lease)
                valid = (
                    waiter.assigned
                    and self._active_call == call_id
                    and self._state is PinState.OPEN
                    and lease_valid
                )
                if not valid:
                    waiter.error = ConnectionClosedError("pinned transport closed during admission")
                    waiter.assigned = False
                    if self._active_call == call_id:
                        self._active_call = None
                        release_failed_lease = self._state is PinState.FAILED
                    if self._state is PinState.OPEN and not lease_valid:
                        self._state = PinState.CLOSED
                    if self._state is not PinState.OPEN or not lease_valid:
                        self._close_waiters_locked()
                    self._condition.notify_all()
            error = waiter.error
        if release_failed_lease:
            self._release_failed_lease()
        if error is not None:
            raise error
        return call_id

    def _wire_terminal(self, call_id: int) -> None:
        wake: list[PinWaiter] = []
        release_failed_lease = False
        with self._condition:
            if self._active_call != call_id:
                return
            self._active_call = None
            if self._state is PinState.FAILED:
                release_failed_lease = True
            elif self._state is not PinState.OPEN or not self._broker.validate(self._lease):
                if self._state is PinState.OPEN:
                    self._state = PinState.CLOSED
                self._close_waiters_locked()
            else:
                while self._waiters:
                    candidate = self._waiters.popleft()
                    if candidate.error is None:
                        if time.monotonic() >= candidate.deadline:
                            candidate.error = ResponseTimeoutError("7709 response timed out during queue")
                            if candidate.reserved:
                                candidate.reserved = False
                                self._broker.release_pin_waiter(candidate.completed)
                            wake.append(candidate)
                            continue
                        if candidate.reserved:
                            candidate.reserved = False
                            self._broker.release_pin_waiter(candidate.completed)
                        self._active_call = candidate.call_id
                        candidate.assigned = True
                        wake.append(candidate)
                        break
            self._condition.notify_all()
        for candidate in wake:
            candidate.completed.set()
        if release_failed_lease:
            self._release_failed_lease()

    def _close_waiters_locked(self) -> None:
        while self._waiters:
            waiter = self._waiters.popleft()
            if waiter.error is None:
                waiter.error = ConnectionClosedError("pinned transport closed during admission")
            if waiter.reserved:
                waiter.reserved = False
                self._broker.release_pin_waiter(waiter.completed)
            waiter.completed.set()

    def _release_failed_lease(self) -> None:
        self._broker.release(self._lease)
        with self._condition:
            if self._state is PinState.FAILED and self._active_call is None:
                self._state = PinState.CLOSED
            self._condition.notify_all()


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
        self._epoch = 0
        self._startup_active = False
        self._startup_cleanup_error: BaseException | None = None
        self._connect_broker: LeaseBroker | None = None
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
        if self._runtime_guard.failure() is not None and state is PoolState.RUNNING:
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

    def connect(self) -> None:
        broker, push_buffer = self._ensure_started()
        connect_deadline = time.monotonic() + self._timeout
        with self._condition:
            while self._connect_broker is broker:
                if self._state is not PoolState.RUNNING or self._broker is not broker:
                    raise ConnectionClosedError("7709 pool changed while waiting to connect")
                remaining = max(0.0, connect_deadline - time.monotonic())
                if remaining == 0 or not self._condition.wait(timeout=remaining):
                    raise ResponseTimeoutError("7709 response timed out during queue")
            if self._state is not PoolState.RUNNING or self._broker is not broker:
                raise ConnectionClosedError("7709 pool changed before connect admission")
            self._connect_broker = broker

        leases: list[SlotLease] = []
        try:
            for _ in range(self._pool_size):
                leases.append(broker.acquire(connect_deadline))
            lease_by_slot = {lease.slot_id: lease for lease in leases}
            first_error: BaseException | None = None
            stopped_early = False
            shutdown_attempt: ShutdownAttempt | None = None
            shutdown_owner = False
            rollback_errors: list[BaseException] = []
            shutdown_error: BaseException | None = None
            with ThreadPoolExecutor(max_workers=self._pool_size, thread_name_prefix="eltdx-pool-connect") as executor:
                futures = [
                    executor.submit(
                        transport._connect_with_deadline,
                        deadline=connect_deadline,
                        completion=None,
                        runtime=transport._runtime,
                        lock_slot=False,
                        lease_id=lease_by_slot[index].lease_id,
                        expected_runtime_epoch=broker.pool_epoch,
                    )
                    for index, transport in enumerate(self._transports)
                ]
                done, _ = wait(futures, return_when=FIRST_EXCEPTION)
                first_error = next((future.exception() for future in done if future.exception() is not None), None)
                if first_error is not None:
                    shutdown_attempt, shutdown_owner = self._claim_shutdown_attempt(
                        expected_broker=broker,
                        expected_push_buffer=push_buffer,
                    )
                    if shutdown_owner:
                        try:
                            self._begin_connect_rollback(broker, push_buffer)
                        except BaseException as exc:
                            rollback_errors.append(exc)
                        stopped_early = True
                        for transport in self._transports:
                            try:
                                transport._request_stop()
                            except BaseException as exc:
                                rollback_errors.append(exc)
                                stopped_early = False
                        try:
                            self._run_shutdown_attempt(
                                shutdown_attempt,
                                already_requested_stop=stopped_early,
                                initial_errors=rollback_errors,
                            )
                        except BaseException as exc:
                            shutdown_error = exc
                wait(futures)
                if shutdown_attempt is not None and not shutdown_owner:
                    try:
                        self._wait_shutdown_attempt(shutdown_attempt)
                    except BaseException as exc:
                        shutdown_error = exc
            errors = [future.exception() for future in futures if future.exception() is not None]
            if errors:
                if shutdown_error is not None:
                    raise shutdown_error
                raise first_error or errors[0]
        finally:
            for lease in leases:
                broker.release(lease)
            with self._condition:
                if self._connect_broker is broker:
                    self._connect_broker = None
                self._condition.notify_all()

    def close(self) -> None:
        self._shutdown(normal=True)

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        broker, _ = self._ensure_started()
        deadline = time.monotonic() + self._timeout
        lease = broker.acquire(deadline)
        completion = LeaseCompletion(broker, lease)
        transport = self._transports[lease.slot_id]
        if not broker.validate(lease):
            completion(None)
            raise ConnectionClosedError("7709 pool lease expired before slot entry")
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
        broker, push_buffer = self._ensure_started()
        lease = broker.acquire(time.monotonic() + self._timeout, pinned=True)
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
        while True:
            with self._condition:
                if self._state is PoolState.RUNNING and self._broker is not None and self._push_buffer is not None:
                    failure = self._runtime_guard.failure()
                    if failure is not None:
                        self._state = PoolState.FAILED
                        raise ConnectionClosedError("7709 pool failed because an Actor terminated") from failure
                    return self._broker, self._push_buffer
                if self._state in (PoolState.CLOSING, PoolState.FAILED, PoolState.FAILED_CLOSING, PoolState.FAILED_CLOSED):
                    raise ConnectionClosedError(f"7709 pool is not usable: {self._state.name}")
                if self._startup_active:
                    self._condition.wait()
                    continue
                self._startup_active = True
                self._startup_cleanup_error = None
                self._state = PoolState.STARTING
                observed_epoch = self._epoch
                candidate_epoch = observed_epoch + 1
                break

        broker: LeaseBroker | None = None
        push_buffer: PushBuffer | None = None
        configured: list[tuple[SocketTransport, RuntimeRegistration]] = []
        try:
            try:
                endpoint_sets = [resolve_hosts(_rotate_hosts(self._hosts, index)) for index in range(self._pool_size)]
            except OSError as exc:
                raise ConnectionClosedError("7709 unable to resolve any configured host") from exc
            with self._condition:
                if self._epoch != observed_epoch or self._state is not PoolState.STARTING:
                    raise ConnectionClosedError("7709 pool changed while resolving endpoints")
            push_buffer = PushBuffer(
                candidate_epoch,
                max_frames=self._push_queue_size,
                max_bytes=self._push_queue_bytes,
            )
            broker = LeaseBroker(candidate_epoch, self._pool_size, self._max_pending_requests)
            heartbeat_allowed = HeartbeatAdmissionGuard(broker)
            self._runtime_guard.configure(broker, push_buffer)
            guard_ref = weakref.ref(self._runtime_guard)
            broker_ref = weakref.ref(broker)
            for transport, endpoints in zip(self._transports, endpoint_sets):
                registration = RuntimeRegistration(guard_ref, candidate_epoch, broker_ref)
                transport._configure_pool_runtime(
                    push_buffer=push_buffer,
                    runtime_epoch=candidate_epoch,
                    endpoints=endpoints,
                    actor_fatal_callback=ActorFatalHandle(guard_ref, candidate_epoch, broker_ref),
                    runtime_started_callback=registration,
                    heartbeat_allowed=heartbeat_allowed,
                )
                configured.append((transport, registration))
        except BaseException:
            cleanup_errors = self._cleanup_unpublished_pool_epoch(
                broker=broker,
                push_buffer=push_buffer,
                runtime_epoch=candidate_epoch,
                configured=configured,
            )
            with self._condition:
                if cleanup_errors:
                    self._broker = broker
                    self._push_buffer = push_buffer
                    self._registrations = tuple(registration for _, registration in configured)
                    self._startup_cleanup_error = cleanup_errors[0]
                    self._epoch = max(self._epoch, candidate_epoch)
                    self._state = PoolState.FAILED_CLOSING
                elif self._state is PoolState.STARTING:
                    self._state = PoolState.STOPPED
                self._startup_active = False
                self._condition.notify_all()
            raise

        with self._condition:
            if self._epoch != observed_epoch or self._state is not PoolState.STARTING:
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
                self._condition.notify_all()
        if not publish:
            cleanup_errors: list[BaseException] = []
            try:
                cleanup_errors = self._cleanup_unpublished_pool_epoch(
                    broker=broker,
                    push_buffer=push_buffer,
                    runtime_epoch=candidate_epoch,
                    configured=configured,
                )
            finally:
                with self._condition:
                    if cleanup_errors:
                        self._broker = broker
                        self._push_buffer = push_buffer
                        self._registrations = tuple(registration for _, registration in configured)
                        self._startup_cleanup_error = cleanup_errors[0]
                        self._epoch = max(self._epoch, candidate_epoch)
                        self._state = PoolState.FAILED_CLOSING
                    self._startup_active = False
                    self._condition.notify_all()
            if cleanup_errors:
                raise cleanup_errors[0]
            raise ConnectionClosedError("7709 pool changed while resolving endpoints")
        return broker, push_buffer

    def _cleanup_unpublished_pool_epoch(
        self,
        *,
        broker: LeaseBroker | None,
        push_buffer: PushBuffer | None,
        runtime_epoch: int,
        configured: Sequence[tuple[SocketTransport, RuntimeRegistration]],
    ) -> list[BaseException]:
        errors: list[BaseException] = []
        for transport, registration in configured:
            try:
                transport._retire_pool_runtime(registration)
            except BaseException as exc:
                errors.append(exc)
        if broker is not None:
            try:
                self._runtime_guard.seal(pool_epoch=runtime_epoch, broker=broker)
            except BaseException as exc:
                errors.append(exc)
            try:
                broker.close()
            except BaseException as exc:
                errors.append(exc)
        if push_buffer is not None:
            try:
                push_buffer.close()
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
                    )
                    if not cleared:
                        all_cleared = False
                        errors.append(RuntimeError("7709 pool startup slot configuration was not cleared"))
                except BaseException as exc:
                    all_cleared = False
                    errors.append(exc)
        if all_cleared and broker is not None:
            try:
                finish_error = self._runtime_guard.finish_epoch(pool_epoch=runtime_epoch, broker=broker)
                if finish_error is not None:
                    errors.append(finish_error)
            except BaseException as exc:
                errors.append(exc)
        return errors

    def _begin_connect_rollback(self, broker: LeaseBroker, push_buffer: PushBuffer) -> None:
        with self._condition:
            if self._broker is not broker or self._push_buffer is not push_buffer:
                return
            if self._state is PoolState.RUNNING:
                self._state = PoolState.CLOSING
                self._epoch += 1
                self._condition.notify_all()
            registrations = self._registrations
        for transport, registration in zip(self._transports, registrations):
            transport._retire_pool_runtime(registration)
        self._runtime_guard.seal(pool_epoch=broker.pool_epoch, broker=broker)
        broker.close()
        push_buffer.close()

    def _shutdown(self, *, normal: bool, already_requested_stop: bool = False) -> None:
        del normal
        attempt, owner = self._claim_shutdown_attempt()
        if attempt is None:
            return
        if not owner:
            self._wait_shutdown_attempt(attempt)
            return
        self._run_shutdown_attempt(attempt, already_requested_stop=already_requested_stop)

    def _claim_shutdown_attempt(
        self,
        *,
        expected_broker: LeaseBroker | None = None,
        expected_push_buffer: PushBuffer | None = None,
    ) -> tuple[ShutdownAttempt | None, bool]:
        with self._condition:
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
            if self._state in (PoolState.STOPPED, PoolState.FAILED_CLOSED):
                return None, False
            self._shutdown_generation += 1
            attempt = ShutdownAttempt(self._shutdown_generation, self._broker, self._push_buffer)
            self._shutdown_attempt = attempt
            self._shutdown_active = True
            return attempt, True

    @staticmethod
    def _wait_shutdown_attempt(attempt: ShutdownAttempt) -> None:
        attempt.completed.wait()
        if attempt.error is not None:
            raise attempt.error

    def _run_shutdown_attempt(
        self,
        attempt: ShutdownAttempt,
        *,
        already_requested_stop: bool,
        initial_errors: Sequence[BaseException] = (),
    ) -> None:

        deadline = time.monotonic() + 1.0
        errors = list(initial_errors)
        failed_before_close = False
        broker: LeaseBroker | None = None
        push_buffer: PushBuffer | None = None
        registrations: tuple[RuntimeRegistration, ...] = ()
        transports = tuple(self._transports)
        normal_cleanup = False
        failed_closed = False

        try:
            with self._condition:
                already_retired = self._state in (PoolState.CLOSING, PoolState.FAILED_CLOSING)
                failed_before_close = self._runtime_guard.failure() is not None or self._state in (
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
                    failed_before_close = failed_before_close or self._runtime_guard.failure() is not None or self._state in (
                        PoolState.FAILED,
                        PoolState.FAILED_CLOSING,
                    )
                    if failed_before_close:
                        self._state = PoolState.FAILED_CLOSING
                broker = self._broker
                push_buffer = self._push_buffer
                registrations = self._registrations

            for transport, registration in zip(transports, registrations):
                try:
                    transport._retire_pool_runtime(registration)
                except BaseException as exc:
                    errors.append(exc)
            if broker is not None:
                try:
                    self._runtime_guard.seal(pool_epoch=broker.pool_epoch, broker=broker)
                except BaseException as exc:
                    errors.append(exc)
                try:
                    broker.close()
                except BaseException as exc:
                    errors.append(exc)
            if push_buffer is not None:
                try:
                    push_buffer.close()
                except BaseException as exc:
                    errors.append(exc)

            if not already_requested_stop:
                for transport in transports:
                    try:
                        transport._request_stop()
                    except BaseException as exc:
                        errors.append(exc)
            for transport in transports:
                try:
                    transport._close_with_timeout(max(0.0, deadline - time.monotonic()))
                except BaseException as exc:
                    errors.append(exc)

            late_failure = self._runtime_guard.failure()
            failed_closed = failed_before_close or late_failure is not None
            if not errors and not failed_closed:
                cleared = True
                if broker is not None and push_buffer is not None:
                    for transport, registration in zip(transports, registrations):
                        try:
                            cleared = transport._clear_pool_runtime(
                                registration=registration,
                                runtime_epoch=broker.pool_epoch,
                                push_buffer=push_buffer,
                            ) and cleared
                        except BaseException as exc:
                            errors.append(exc)
                            cleared = False
                    if cleared and not errors:
                        finish_error = self._runtime_guard.finish_epoch(
                            pool_epoch=broker.pool_epoch,
                            broker=broker,
                        )
                        if finish_error is not None:
                            failed_closed = True
                if cleared and not errors and not failed_closed:
                    normal_cleanup = True
        except BaseException as exc:
            errors.append(exc)
        finally:
            with self._condition:
                if errors:
                    self._state = PoolState.FAILED_CLOSING
                elif failed_closed:
                    self._state = PoolState.FAILED_CLOSED
                elif normal_cleanup:
                    self._state = PoolState.STOPPED
                    self._broker = None
                    self._push_buffer = None
                    self._registrations = ()
                else:
                    self._state = PoolState.FAILED_CLOSING
                    errors.append(TransportCloseTimeoutError("7709 pool shutdown did not reach a terminal state"))
                attempt.error = errors[0] if errors else None
                attempt.broker = None
                attempt.push_buffer = None
                self._shutdown_active = False
                attempt.completed.set()
                self._condition.notify_all()

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
