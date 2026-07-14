"""Bounded FIFO connection pool for the 7709 Actor transport."""

from __future__ import annotations

import threading
import time
import weakref
from collections import deque
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, wait
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
        self._closed = False
        self._waiter_local = threading.local()

    def acquire(self, deadline: float, *, pinned: bool = False) -> SlotLease:
        with self._condition:
            if self._closed:
                raise ConnectionClosedError("7709 pool is closed")
            if self._idle_slots and not self._waiters:
                return self._new_lease_locked(self._idle_slots.popleft(), pinned)
            if len(self._waiters) + self._pin_waiters >= self.max_pending_requests:
                raise PoolBusyError("7709 pool admission queue is full")
            self._waiter_counter += 1
            completed = getattr(self._waiter_local, "completed", None)
            if completed is None:
                completed = threading.Event()
                self._waiter_local.completed = completed
            else:
                completed.clear()
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
        if waiter.state is AdmissionState.ASSIGNED and waiter.assigned_lease is not None:
            return waiter.assigned_lease
        if waiter.error is not None:
            raise waiter.error
        raise ResponseTimeoutError("7709 response timed out during queue")

    def release(self, lease: SlotLease) -> bool:
        wake: AdmissionWaiter | None = None
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
                waiter.assigned_lease = self._new_lease_locked(lease.slot_id, waiter.pinned)
                waiter.state = AdmissionState.ASSIGNED
                wake = waiter
                break
            else:
                self._idle_slots.append(lease.slot_id)
        if wake is not None:
            wake.completed.set()
        return True

    def validate(self, lease: SlotLease) -> bool:
        with self._condition:
            return (
                not self._closed
                and lease.pool_epoch == self.pool_epoch
                and lease.state is LeaseState.ACTIVE
                and self._active_leases.get(lease.lease_id) is lease
            )

    def reserve_pin_waiter(self) -> None:
        with self._condition:
            if self._closed:
                raise ConnectionClosedError("7709 pool is closed")
            if len(self._waiters) + self._pin_waiters >= self.max_pending_requests:
                raise PoolBusyError("7709 pool admission queue is full")
            self._pin_waiters += 1
            self._condition.notify_all()

    def release_pin_waiter(self) -> None:
        with self._condition:
            if self._pin_waiters > 0:
                self._pin_waiters -= 1

    def close(self) -> None:
        wake: list[AdmissionWaiter] = []
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
            self._pin_waiters = 0
        for waiter in wake:
            waiter.completed.set()

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


class PoolRuntimeGuard:
    """Facade-independent runtime group used by fatal paths and finalization."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._broker: LeaseBroker | None = None
        self._push_buffer: PushBuffer | None = None
        self._runtimes: list[ActorRuntime] = []
        self._fatal_error: BaseException | None = None

    def configure(self, broker: LeaseBroker, push_buffer: PushBuffer) -> None:
        with self._lock:
            self._broker = broker
            self._push_buffer = push_buffer
            self._runtimes = []
            self._fatal_error = None

    def add_runtime(self, runtime: ActorRuntime) -> None:
        with self._lock:
            if all(item is not runtime for item in self._runtimes):
                self._runtimes.append(runtime)

    def fail(self, runtime: ActorRuntime, error: BaseException) -> None:
        with self._lock:
            if self._fatal_error is None:
                self._fatal_error = error
            if all(item is not runtime for item in self._runtimes):
                self._runtimes.append(runtime)
            broker = self._broker
            push_buffer = self._push_buffer
            runtimes = tuple(self._runtimes)
        if broker is not None:
            broker.close()
        if push_buffer is not None:
            push_buffer.close(error)
        for item in runtimes:
            request_actor_stop(item)

    def abandon(self) -> None:
        with self._lock:
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

    def finish_epoch(self) -> None:
        with self._lock:
            self._broker = None
            self._push_buffer = None
            self._runtimes = []
            self._fatal_error = None


@dataclass(frozen=True, slots=True)
class ActorFatalHandle:
    guard_ref: weakref.ReferenceType[PoolRuntimeGuard]

    def __call__(self, runtime: ActorRuntime, error: BaseException) -> None:
        guard = self.guard_ref()
        if guard is not None:
            guard.fail(runtime, error)


@dataclass(frozen=True, slots=True)
class RuntimeRegistration:
    guard_ref: weakref.ReferenceType[PoolRuntimeGuard]

    def __call__(self, runtime: ActorRuntime) -> None:
        guard = self.guard_ref()
        if guard is not None:
            guard.add_runtime(runtime)


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
class PinWaiter:
    call_id: int
    deadline: float
    completed: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    reserved: bool = False
    assigned: bool = False


class PinCompletion:
    def __init__(self, proxy: PinnedTransportProxy, call_id: int) -> None:
        self._proxy_ref = weakref.ref(proxy)
        self._call_id = call_id
        self._lock = threading.Lock()
        self._done = False

    def __call__(self, ticket: object | None) -> None:
        with self._lock:
            if self._done:
                return
            self._done = True
        proxy = self._proxy_ref()
        if proxy is not None:
            proxy._wire_terminal(self._call_id)


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
        self._closed = False

    @property
    def connected_host(self) -> str | None:
        self._validate()
        return self._slot.connected_host

    @property
    def pending_push_count(self) -> int:
        self._validate()
        return self._push_buffer.pending_count

    def connect(self) -> None:
        self._validate()
        self._slot.connect()

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        deadline = time.monotonic() + self._timeout
        call_id = self._admit(deadline)
        completion = PinCompletion(self, call_id)
        runtime = self._slot._runtime
        try:
            return self._slot._execute_with_lease(
                command,
                payload,
                lease_id=self._lease.lease_id,
                deadline=deadline,
                completion=completion,
                runtime=runtime,
                lock_slot=False,
            )
        except BaseException:
            completion(None)
            raise

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
        wake: list[PinWaiter] = []
        with self._condition:
            if self._closed:
                return
            self._closed = True
            while self._waiters:
                waiter = self._waiters.popleft()
                waiter.error = ConnectionClosedError("pinned transport closed")
                wake.append(waiter)
            active = self._active_call
        for waiter in wake:
            if waiter.reserved:
                self._broker.release_pin_waiter()
            waiter.completed.set()
        if active is not None:
            self._slot._cancel_lease(self._lease.lease_id)
            deadline = time.monotonic() + min(1.0, self._timeout)
            with self._condition:
                if not self._condition.wait_for(lambda: self._active_call is None, timeout=max(0.0, deadline - time.monotonic())):
                    raise TransportCloseTimeoutError("pinned transport did not quiesce")
        self._broker.release(self._lease)

    def _validate(self) -> None:
        with self._condition:
            closed = self._closed
        if closed or not self._broker.validate(self._lease):
            raise ConnectionClosedError("pinned transport lease is no longer valid")

    def _admit(self, deadline: float) -> int:
        self._validate()
        with self._condition:
            self._call_counter += 1
            call_id = self._call_counter
            if self._active_call is None and not self._waiters:
                self._active_call = call_id
                return call_id
        self._broker.reserve_pin_waiter()
        waiter = PinWaiter(call_id, deadline, reserved=True)
        release_reservation = False
        with self._condition:
            if self._closed:
                release_reservation = True
                waiter.error = ConnectionClosedError("pinned transport closed")
            elif self._active_call is None and not self._waiters:
                self._active_call = call_id
                release_reservation = True
                waiter.completed.set()
            else:
                self._waiters.append(waiter)
        if release_reservation:
            self._broker.release_pin_waiter()
        if waiter.error is not None:
            raise waiter.error
        if waiter.completed.is_set():
            return call_id
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
                self._broker.release_pin_waiter()
                waiter.reserved = False
        if waiter.error is not None:
            raise waiter.error
        return call_id

    def _wire_terminal(self, call_id: int) -> None:
        next_waiter: PinWaiter | None = None
        with self._condition:
            if self._active_call != call_id:
                return
            self._active_call = None
            while self._waiters:
                candidate = self._waiters.popleft()
                if candidate.error is None:
                    next_waiter = candidate
                    self._active_call = candidate.call_id
                    candidate.assigned = True
                    candidate.completed.set()
                    break
            self._condition.notify_all()
        if next_waiter is not None:
            if next_waiter.reserved:
                self._broker.release_pin_waiter()
                next_waiter.reserved = False


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
        self._broker: LeaseBroker | None = None
        self._push_buffer: PushBuffer | None = None
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
        self._ensure_started()
        with ThreadPoolExecutor(max_workers=self._pool_size, thread_name_prefix="eltdx-pool-connect") as executor:
            futures = [executor.submit(transport.connect) for transport in self._transports]
            wait(futures)
        errors = [future.exception() for future in futures if future.exception() is not None]
        if errors:
            self._shutdown(normal=True)
            raise errors[0]

    def close(self) -> None:
        self._shutdown(normal=True)

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        broker, _ = self._ensure_started()
        deadline = time.monotonic() + self._timeout
        lease = broker.acquire(deadline)
        completion = LeaseCompletion(broker, lease)
        transport = self._transports[lease.slot_id]
        return transport._execute_with_lease(
            command,
            payload,
            lease_id=lease.lease_id,
            deadline=deadline,
            completion=completion,
            runtime=transport._runtime,
            lock_slot=False,
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
                self._state = PoolState.STARTING
                observed_epoch = self._epoch
                candidate_epoch = observed_epoch + 1
                break

        broker: LeaseBroker | None = None
        push_buffer: PushBuffer | None = None
        try:
            endpoint_sets = [resolve_hosts(_rotate_hosts(self._hosts, index)) for index in range(self._pool_size)]
            with self._condition:
                if self._epoch != observed_epoch or self._state is not PoolState.STARTING:
                    raise ConnectionClosedError("7709 pool changed while resolving endpoints")
            push_buffer = PushBuffer(
                candidate_epoch,
                max_frames=self._push_queue_size,
                max_bytes=self._push_queue_bytes,
            )
            broker = LeaseBroker(candidate_epoch, self._pool_size, self._max_pending_requests)
            self._runtime_guard.configure(broker, push_buffer)
            guard_ref = weakref.ref(self._runtime_guard)
            for transport, endpoints in zip(self._transports, endpoint_sets):
                transport._configure_pool_runtime(
                    push_buffer=push_buffer,
                    runtime_epoch=candidate_epoch,
                    endpoints=endpoints,
                    actor_fatal_callback=ActorFatalHandle(guard_ref),
                    runtime_started_callback=RuntimeRegistration(guard_ref),
                )
        except BaseException:
            if broker is not None:
                broker.close()
            if push_buffer is not None:
                push_buffer.close()
            self._runtime_guard.finish_epoch()
            with self._condition:
                self._startup_active = False
                if self._state is PoolState.STARTING:
                    self._state = PoolState.STOPPED
                self._condition.notify_all()
            raise

        with self._condition:
            if self._epoch != observed_epoch or self._state is not PoolState.STARTING:
                publish = False
            else:
                self._epoch = candidate_epoch
                self._broker = broker
                self._push_buffer = push_buffer
                self._state = PoolState.RUNNING
                publish = True
            self._startup_active = False
            self._condition.notify_all()
        if not publish:
            broker.close()
            push_buffer.close()
            self._runtime_guard.finish_epoch()
            raise ConnectionClosedError("7709 pool changed while resolving endpoints")
        return broker, push_buffer

    def _shutdown(self, *, normal: bool) -> None:
        with self._condition:
            cancelled_startup = self._startup_active
            if self._startup_active:
                self._state = PoolState.CLOSING
                self._epoch += 1
                self._condition.notify_all()
            while self._startup_active:
                self._condition.wait()
            if self._state is PoolState.STOPPED:
                return
            if self._state is PoolState.FAILED_CLOSED:
                return
            failed_before_close = self._runtime_guard.failure() is not None or self._state in (
                PoolState.FAILED,
                PoolState.FAILED_CLOSING,
            )
            self._state = PoolState.CLOSING
            if not cancelled_startup:
                self._epoch += 1
            broker = self._broker
            push_buffer = self._push_buffer
            transports = tuple(self._transports)
        if broker is not None:
            broker.close()
        if push_buffer is not None:
            push_buffer.close()
        for transport in transports:
            transport._request_stop()
        deadline = time.monotonic() + 1.0
        errors: list[BaseException] = []
        for transport in transports:
            try:
                transport._close_with_timeout(max(0.0, deadline - time.monotonic()))
            except BaseException as exc:
                errors.append(exc)
        with self._condition:
            if errors:
                self._state = PoolState.FAILED_CLOSING
            elif failed_before_close:
                self._state = PoolState.FAILED_CLOSED
            else:
                self._state = PoolState.STOPPED
                self._broker = None
                self._push_buffer = None
                self._runtime_guard.finish_epoch()
            self._condition.notify_all()
        if errors:
            raise errors[0]

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
