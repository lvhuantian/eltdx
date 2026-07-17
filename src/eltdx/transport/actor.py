"""Single-threaded non-blocking connection Actor for the 7709 transport."""

from __future__ import annotations

import errno
import hashlib
import secrets
import selectors
import socket
import threading
import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any

from eltdx.exceptions import (
    ConnectionClosedError,
    ProtocolError,
    ResponseTimeoutError,
    TransportCloseTimeoutError,
    TransportError,
)
from eltdx.hosts import ResolvedEndpoint
from eltdx.protocol.commands import build_command_frame, parse_command_response
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_HEARTBEAT
from eltdx.protocol.frame import RESPONSE_HEADER_SIZE, ResponseFrame, ResponseFrameDecoder

from .push import PushBuffer, PushDropPublication, PushFrame


SelectorFactory = Callable[[], selectors.BaseSelector]
SocketFactory = Callable[[int, int, int], socket.socket]
CandidateCallback = Callable[["ActorRuntime"], None]
_IDENTITY_GATE_RECHECK_INTERVAL = 0.05


@dataclass(slots=True, eq=False)
class _IdentityWaiter:
    token: object
    deadline: float | None
    event: threading.Event = field(default_factory=threading.Event)
    granted: bool = False
    terminal: bool = False


@dataclass(slots=True, eq=False)
class _IdentityOwnerPublication:
    token: object
    released: bool = False


class IdentityGate:
    """Cross-thread gate whose owner is released by exact token identity."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._state_lock = threading.RLock()
        self._owner: object | None = None
        self._owner_publication: _IdentityOwnerPublication | None = None
        self._waiters: deque[_IdentityWaiter] = deque()
        self._waiter_snapshot: tuple[_IdentityWaiter, ...] = ()
        self._compat = threading.local()

    def _state_is_owned(self) -> bool:
        checker = getattr(self._state_lock, "_is_owned", None)
        return bool(checker()) if checker is not None else False

    def _release_condition(self) -> None:
        while True:
            try:
                self._condition.release()
                return
            except RuntimeError:
                return
            except BaseException:
                continue

    def _release_state(self) -> None:
        while True:
            try:
                self._state_lock.release()
                return
            except RuntimeError:
                return
            except BaseException:
                if not self._state_is_owned():
                    return

    def _acquire_state(self, deadline: float | None, *, uninterruptible: bool = False) -> bool:
        while True:
            timeout = None if deadline is None else max(0.0, deadline - time.monotonic())
            try:
                if timeout is None:
                    acquired = self._state_lock.acquire()
                elif timeout == 0:
                    acquired = self._state_lock.acquire(blocking=False)
                else:
                    acquired = self._state_lock.acquire(timeout=timeout)
            except BaseException:
                if self._state_is_owned():
                    if uninterruptible:
                        return True
                    self._release_state()
                if uninterruptible:
                    continue
                raise
            return bool(acquired)

    def acquire_token(self, token: object, deadline: float | None) -> bool:
        timeout = None if deadline is None else max(0.0, deadline - time.monotonic())
        try:
            acquired = self._condition.acquire() if timeout is None else self._condition.acquire(timeout=timeout)
        except BaseException:
            self._release_condition()
            raise
        if not acquired:
            return False
        state_acquired = False
        try:
            state_acquired = self._acquire_state(deadline)
            if not state_acquired:
                return False
            try:
                self._apply_published_release_locked()
                if self._owner is None and not self._waiters:
                    self._set_owner_locked(token)
                    return True
                if deadline is not None and deadline <= time.monotonic():
                    return False
                waiter = _IdentityWaiter(token, deadline)
                self._waiters.append(waiter)
                self._waiter_snapshot = tuple(self._waiters)
            finally:
                self._release_state()
                state_acquired = False
        finally:
            self._release_condition()

        try:
            while True:
                remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                wait_for = (
                    _IDENTITY_GATE_RECHECK_INTERVAL
                    if remaining is None
                    else min(_IDENTITY_GATE_RECHECK_INTERVAL, remaining)
                )
                if wait_for > 0:
                    waiter.event.wait(wait_for)
                state_acquired = self._acquire_state(deadline)
                if not state_acquired:
                    state_acquired = self._acquire_state(None, uninterruptible=True)
                try:
                    self._apply_published_release_locked()
                    if waiter.granted and self._owner is token:
                        return True
                    if waiter.terminal:
                        return False
                    if deadline is not None and deadline <= time.monotonic():
                        self._remove_waiter_locked(waiter)
                        waiter.terminal = True
                        return False
                finally:
                    self._release_state()
                    state_acquired = False
        except BaseException:
            self._withdraw_waiter(waiter, abandon_owner=True)
            raise

    def _remove_waiter_locked(self, waiter: _IdentityWaiter) -> None:
        try:
            self._waiters.remove(waiter)
        except ValueError:
            pass
        self._waiter_snapshot = tuple(self._waiters)

    def _handoff_locked(self, token: object) -> bool:
        if self._owner is not token:
            return False
        self._owner = None
        self._owner_publication = None
        while self._waiters:
            waiter = self._waiters.popleft()
            if waiter.terminal:
                continue
            if waiter.deadline is not None and waiter.deadline <= time.monotonic():
                waiter.terminal = True
                try:
                    waiter.event.set()
                except BaseException:
                    pass
                continue
            self._set_owner_locked(waiter.token)
            waiter.granted = True
            try:
                waiter.event.set()
            except BaseException:
                waiter.granted = False
                waiter.terminal = True
                self._owner = None
                continue
            break
        self._waiter_snapshot = tuple(self._waiters)
        return True

    def _withdraw_waiter(self, waiter: _IdentityWaiter, *, abandon_owner: bool) -> None:
        self._acquire_state(None, uninterruptible=True)
        try:
            if abandon_owner and waiter.granted and self._owner is waiter.token:
                waiter.granted = False
                waiter.terminal = True
                self._handoff_locked(waiter.token)
            else:
                self._remove_waiter_locked(waiter)
                waiter.terminal = True
        finally:
            self._release_state()

    def release_token(self, token: object) -> bool:
        self._acquire_state(None, uninterruptible=True)
        try:
            self._apply_published_release_locked()
            released = self._handoff_locked(token)
        finally:
            self._release_state()
        return released

    def publish_release_token(self, token: object) -> None:
        if self._state_lock.acquire(blocking=False):
            try:
                self._apply_published_release_locked()
                self._handoff_locked(token)
            finally:
                self._state_lock.release()
            return
        publication = self._owner_publication
        if publication is None or publication.token is not token:
            return
        publication.released = True
        for waiter in self._waiter_snapshot:
            waiter.event.set()

    def _apply_published_release_locked(self) -> None:
        publication = self._owner_publication
        if publication is None or not publication.released:
            return
        if self._owner is publication.token:
            self._handoff_locked(publication.token)

    def _set_owner_locked(self, token: object) -> None:
        self._owner = token
        self._owner_publication = _IdentityOwnerPublication(token)

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        token = object()
        if not blocking:
            deadline = time.monotonic()
        elif timeout >= 0:
            deadline = time.monotonic() + timeout
        else:
            deadline = None
        acquired = self.acquire_token(token, deadline)
        if acquired:
            self._compat.token = token
        return acquired

    def release(self) -> None:
        token = getattr(self._compat, "token", None)
        if token is None or not self.release_token(token):
            raise RuntimeError("cannot release an unowned identity gate")
        self._compat.token = None

    def __enter__(self) -> IdentityGate:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


class ActorStartupError(TransportError):
    def __init__(self, message: str, runtime: ActorRuntime) -> None:
        super().__init__(message)
        self.runtime = runtime


class RuntimeState(Enum):
    STARTING = auto()
    RUNNING = auto()
    CLOSING = auto()
    STOPPED = auto()
    FAILED = auto()
    FAILED_CLOSING = auto()
    FAILED_CLOSED = auto()


class TcpState(Enum):
    DOWN = auto()
    CONNECTING = auto()
    CONNECTED_UNHANDSHAKEN = auto()
    HANDSHAKING = auto()
    READY = auto()
    RETIRING = auto()


class RequestState(Enum):
    ADMITTED = auto()
    SENDING = auto()
    WAITING_RESPONSE = auto()
    SUCCESS = auto()
    FAILED = auto()
    CANCELLED = auto()


TERMINAL_REQUEST_STATES = frozenset((RequestState.SUCCESS, RequestState.FAILED, RequestState.CANCELLED))


@dataclass(slots=True)
class ConnectTicket:
    runtime_epoch: int
    deadline: float
    lease_id: int = 0
    request_id: int = 0
    completion: Callable[[ConnectTicket | None], None] | None = None
    state: RequestState = RequestState.ADMITTED
    connected_host: str | None = None
    error: BaseException | None = None
    completed_at: float | None = None
    completion_error: BaseException | None = None
    terminal_claimed: bool = False
    completion_settled: bool = False
    completed: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass(slots=True)
class RequestTicket:
    runtime_epoch: int
    lease_id: int
    command: int
    request_payload_snapshot: object
    deadline: float
    retry_safe: bool
    request_id: int = 0
    completion: Callable[[RequestTicket | None], None] | None = None
    internal: bool = False
    attempts: int = 0
    attempt_deadline: float = 0.0
    next_endpoint_index: int = 0
    state: RequestState = RequestState.ADMITTED
    result: object | None = None
    error: BaseException | None = None
    completed_at: float | None = None
    completion_error: BaseException | None = None
    terminal_claimed: bool = False
    completion_settled: bool = False
    completed: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass(slots=True)
class TcpGeneration:
    generation_id: int
    sock: socket.socket
    endpoint: ResolvedEndpoint
    state: TcpState
    endpoint_index: int = 0
    connect_deadline: float = 0.0
    connect_recheck_at: float = 0.0
    decoder: ResponseFrameDecoder = field(default_factory=ResponseFrameDecoder)
    tx_bytes: bytes = b""
    tx_offset: int = 0
    active_exchange: WireExchange | None = None
    decoded_frames: deque[ReceivedFrame] = field(default_factory=deque)
    rx_sequence: int = 0
    exchange_counter: int = 0
    receive_drained: bool = False
    connected_at: float = 0.0
    last_activity_at: float = 0.0
    selector_events: int = 0


@dataclass(slots=True)
class WireExchange:
    ticket: RequestTicket
    command: int
    msg_id: int
    msg_type: int
    frame: bytes
    handshake: bool
    rx_boundary: int = 0
    exchange_id: int = 0
    sent_any: bool = False
    send_claimed: bool = False


@dataclass(frozen=True, slots=True)
class CancelToken:
    runtime_epoch: int
    request_id: int
    lease_id: int


@dataclass(frozen=True, slots=True)
class ReceivedFrame:
    sequence: int
    response: ResponseFrame
    exchange_id: int | None = None
    send_complete: bool = False


@dataclass(frozen=True, slots=True)
class FrameEnvelope:
    runtime_epoch: int
    tcp_generation: int
    lease_id: int
    request_id: int
    msg_id: int
    msg_type: int
    command: int
    connected_host: str
    request_payload_snapshot: object
    response: ResponseFrame


@dataclass(frozen=True, slots=True)
class SelectorToken:
    kind: str
    runtime_epoch: int
    tcp_generation: int
    sock: object


@dataclass(frozen=True, slots=True)
class ActorSnapshot:
    runtime_epoch: int
    state: RuntimeState
    tcp_state: TcpState
    tcp_generation: int
    connected_host: str | None
    actor_alive: bool
    pending_depth: int
    reconnect_count: int
    stale_event_count: int
    last_error: str | None


@dataclass(slots=True, eq=False)
class ActorRuntime:
    runtime_epoch: int
    endpoints: tuple[ResolvedEndpoint, ...]
    selector_factory: SelectorFactory = selectors.DefaultSelector
    socket_factory: SocketFactory = socket.socket
    push_buffer: PushBuffer | None = None
    heartbeat_interval: float | None = None
    heartbeat_allowed: Callable[[], bool] | None = None
    request_timeout: float = 8.0
    owns_push_buffer: bool = True
    owner_settles_push: bool = False
    fatal_callback: Callable[[ActorRuntime, BaseException], None] | None = None
    fatal_settled: bool = False
    fatal_settlement_lock: threading.Lock = field(default_factory=threading.Lock)
    fatal_settlement_error: BaseException | None = None
    unreported_fatal_settlement_error: BaseException | None = None
    successor_grace: float = 0.0
    terminal_yield: bool = False
    control_lock: IdentityGate = field(default_factory=IdentityGate)
    control_ready: threading.Event = field(default_factory=threading.Event)
    state: RuntimeState = RuntimeState.STARTING
    stop_requested: bool = False
    pending_task: ConnectTicket | RequestTicket | None = None
    cancel_requests: dict[int, CancelToken] = field(default_factory=dict)
    started: threading.Event = field(default_factory=threading.Event)
    stopped: threading.Event = field(default_factory=threading.Event)
    generation_started: threading.Event = field(default_factory=threading.Event)
    fatal_error: BaseException | None = None
    cleanup_error: BaseException | None = None
    push_cleanup_error: BaseException | None = None
    deferred_cleanup_error: BaseException | None = None
    actor_thread: threading.Thread | None = None
    selector: selectors.BaseSelector | None = None
    wake_reader: socket.socket | None = None
    wake_writer: socket.socket | None = None
    generation: TcpGeneration | None = None
    active_task: ConnectTicket | RequestTicket | None = None
    endpoint_index: int = 0
    endpoints_remaining: int = 0
    generation_counter: int = 0
    reconnect_count: int = 0
    connected_host: str | None = None
    last_error: BaseException | None = None
    msg_id_counter: int = 0
    msg_id_key: bytes = field(default_factory=lambda: secrets.token_bytes(16))
    request_id_counter: int = 0
    stale_event_count: int = 0
    last_handshake: object | None = None
    last_heartbeat: object | None = None
    push_drop_publication: PushDropPublication = field(default_factory=PushDropPublication)

    def __post_init__(self) -> None:
        if self.push_buffer is not None:
            self.push_buffer.register_drop_publication(
                self.push_drop_publication,
                deadline=time.monotonic(),
            )


_SUCCESS_CONNECT_CODES = frozenset(
    value for value in (0, getattr(errno, "EISCONN", None), getattr(socket, "EISCONN", None)) if value is not None
)
_IN_PROGRESS_CONNECT_CODES = frozenset(
    value
    for value in (
        getattr(errno, "EINPROGRESS", None),
        getattr(errno, "EWOULDBLOCK", None),
        getattr(errno, "EALREADY", None),
        getattr(errno, "EINTR", None),
        getattr(errno, "WSAEINPROGRESS", None),
        getattr(errno, "WSAEWOULDBLOCK", None),
        getattr(errno, "WSAEALREADY", None),
        getattr(errno, "WSAEINTR", None),
        getattr(socket, "EINPROGRESS", None),
        getattr(socket, "EWOULDBLOCK", None),
        getattr(socket, "EALREADY", None),
        getattr(socket, "EINTR", None),
        getattr(socket, "WSAEINPROGRESS", None),
        getattr(socket, "WSAEWOULDBLOCK", None),
        getattr(socket, "WSAEALREADY", None),
        getattr(socket, "WSAEINTR", None),
    )
    if value is not None
)

_MAX_REQUEST_ATTEMPTS = 2
_CONNECT_RECHECK_INTERVAL = 0.01
_MAX_DECODED_FRAMES = 1024
_MAX_RECEIVE_CHUNK = RESPONSE_HEADER_SIZE * _MAX_DECODED_FRAMES


def start_actor(
    runtime_epoch: int,
    endpoints: Sequence[ResolvedEndpoint],
    *,
    selector_factory: SelectorFactory = selectors.DefaultSelector,
    socket_factory: SocketFactory = socket.socket,
    push_buffer: PushBuffer | None = None,
    heartbeat_interval: float | None = None,
    heartbeat_allowed: Callable[[], bool] | None = None,
    request_timeout: float = 8.0,
    owns_push_buffer: bool = True,
    owner_settles_push: bool = False,
    fatal_callback: Callable[[ActorRuntime, BaseException], None] | None = None,
    successor_grace: float = 0.0,
    terminal_yield: bool = False,
    candidate_callback: CandidateCallback | None = None,
    startup_timeout: float = 1.0,
) -> ActorRuntime:
    startup_deadline = time.monotonic() + max(0.0, startup_timeout)
    runtime = ActorRuntime(
        runtime_epoch=runtime_epoch,
        endpoints=tuple(endpoints),
        selector_factory=selector_factory,
        socket_factory=socket_factory,
        push_buffer=push_buffer,
        heartbeat_interval=heartbeat_interval,
        heartbeat_allowed=heartbeat_allowed,
        request_timeout=request_timeout,
        owns_push_buffer=owns_push_buffer,
        owner_settles_push=owner_settles_push,
        fatal_callback=fatal_callback,
        successor_grace=max(0.0, successor_grace),
        terminal_yield=terminal_yield,
    )
    try:
        if push_buffer is not None and not push_buffer.register_drop_publication(
            runtime.push_drop_publication,
            deadline=startup_deadline,
        ):
            raise TransportCloseTimeoutError("7709 push publication registration timed out")
        thread = threading.Thread(
            target=_run_actor,
            args=(runtime,),
            name=f"eltdx-7709-actor-{runtime_epoch}",
            daemon=True,
        )
        runtime.actor_thread = thread
        if candidate_callback is not None:
            candidate_callback(runtime)
        thread.start()
    except BaseException as exc:
        _fail_actor_startup(runtime, exc, deadline=startup_deadline)
        raise ActorStartupError("7709 Actor failed before thread startup", runtime) from exc
    if not runtime.started.wait(max(0.0, startup_deadline - time.monotonic())):
        request_actor_stop(runtime)
        raise ActorStartupError("7709 Actor failed to start", runtime)
    if runtime.fatal_error is not None:
        _settle_fatal(runtime, deadline=startup_deadline)
        raise ActorStartupError("7709 Actor failed during startup", runtime) from runtime.fatal_error
    return runtime


def submit_connect(
    runtime: ActorRuntime,
    deadline: float,
    lease_id: int = 0,
    completion: Callable[[ConnectTicket | None], None] | None = None,
    submission_claim: Callable[[ConnectTicket], None] | None = None,
) -> ConnectTicket:
    control_token = _acquire_control_lock(runtime, deadline, "connect submission", timeout_error=True)
    try:
        if runtime.state is not RuntimeState.RUNNING or runtime.stop_requested:
            raise ConnectionClosedError(f"7709 Actor is not running: {runtime.state.name}")
        if runtime.pending_task is not None:
            raise TransportError("7709 Actor mailbox is full")
        runtime.request_id_counter += 1
        ticket = ConnectTicket(
            runtime_epoch=runtime.runtime_epoch,
            deadline=deadline,
            lease_id=lease_id,
            request_id=runtime.request_id_counter,
            completion=completion,
        )
        if submission_claim is not None:
            submission_claim(ticket)
        runtime.pending_task = ticket
        writer = runtime.wake_writer
    finally:
        runtime.control_lock.release_token(control_token)
    _notify_submitted_ticket(runtime, ticket, writer)
    return ticket


def submit_request(
    runtime: ActorRuntime,
    *,
    lease_id: int,
    command: int,
    payload: object,
    deadline: float,
    retry_safe: bool,
    completion: Callable[[RequestTicket | None], None] | None = None,
    submission_claim: Callable[[RequestTicket], None] | None = None,
) -> RequestTicket:
    control_token = _acquire_control_lock(runtime, deadline, "request submission", timeout_error=True)
    try:
        if runtime.state is not RuntimeState.RUNNING or runtime.stop_requested:
            raise ConnectionClosedError(f"7709 Actor is not running: {runtime.state.name}")
        if runtime.pending_task is not None:
            raise TransportError("7709 Actor mailbox is full")
        runtime.request_id_counter += 1
        ticket = RequestTicket(
            runtime_epoch=runtime.runtime_epoch,
            lease_id=lease_id,
            command=command,
            request_payload_snapshot=payload,
            deadline=deadline,
            retry_safe=retry_safe,
            request_id=runtime.request_id_counter,
            completion=completion,
        )
        if submission_claim is not None:
            submission_claim(ticket)
        runtime.pending_task = ticket
        writer = runtime.wake_writer
    finally:
        runtime.control_lock.release_token(control_token)
    _notify_submitted_ticket(runtime, ticket, writer)
    return ticket


def cancel_ticket(
    runtime: ActorRuntime,
    ticket: ConnectTicket | RequestTicket,
    *,
    deadline: float | None = None,
) -> bool:
    control_token = _acquire_control_lock(runtime, deadline, "cancel")
    try:
        with ticket.lock:
            terminal_claimed = ticket.terminal_claimed
            terminal = ticket.state in TERMINAL_REQUEST_STATES
        if (
            ticket.runtime_epoch != runtime.runtime_epoch
            or terminal_claimed
            or (ticket is not runtime.active_task and ticket is not runtime.pending_task)
        ):
            return terminal_claimed and not terminal
        runtime.cancel_requests[ticket.request_id] = CancelToken(
            runtime_epoch=runtime.runtime_epoch,
            request_id=ticket.request_id,
            lease_id=ticket.lease_id,
        )
        writer = runtime.wake_writer
    finally:
        runtime.control_lock.release_token(control_token)
    _notify_actor(runtime, writer)
    return True


def wait_ticket(ticket: ConnectTicket | RequestTicket) -> Any:
    remaining = max(0.0, ticket.deadline - time.monotonic())
    ticket.completed.wait(remaining)
    if not ticket.completed.is_set():
        if isinstance(ticket, ConnectTicket):
            stage = "connect"
        elif ticket.state is RequestState.WAITING_RESPONSE:
            stage = "response"
        elif ticket.state is RequestState.SENDING:
            stage = "send"
        else:
            stage = "Actor completion"
        raise ResponseTimeoutError(f"7709 response timed out during {stage}")
    _settle_ticket_completion(ticket)
    completion_error = ticket.completion_error
    propagate_completion_error = completion_error is not None and getattr(
        ticket.completion,
        "propagate_settlement_error",
        False,
    )
    terminal_error: BaseException | None = None
    if ticket.state is RequestState.SUCCESS and ticket.completed_at is not None and ticket.completed_at > ticket.deadline:
        terminal_error = ResponseTimeoutError("7709 response timed out during Actor completion")
    elif ticket.error is not None:
        terminal_error = ticket.error
    if terminal_error is not None:
        if propagate_completion_error:
            raise terminal_error from completion_error
        raise terminal_error
    if propagate_completion_error:
        raise completion_error
    if isinstance(ticket, ConnectTicket):
        return ticket.connected_host
    return ticket.result


def request_actor_stop(runtime: ActorRuntime, *, deadline: float | None = None) -> None:
    control_token = _acquire_control_lock(runtime, deadline, "stop")
    try:
        runtime.stop_requested = True
        thread = runtime.actor_thread
        if runtime.state in (RuntimeState.STARTING, RuntimeState.RUNNING) or (
            runtime.state is RuntimeState.FAILED and thread is not None and thread.is_alive()
        ):
            runtime.state = RuntimeState.CLOSING
        writer = runtime.wake_writer
    finally:
        runtime.control_lock.release_token(control_token)
    _notify_actor(runtime, writer)


def _acquire_control_lock(
    runtime: ActorRuntime,
    deadline: float | None,
    operation: str,
    *,
    timeout_error: bool = False,
) -> object:
    token = object()
    try:
        acquired = runtime.control_lock.acquire_token(token, deadline)
    except BaseException:
        runtime.control_lock.release_token(token)
        raise
    if acquired:
        return token
    if timeout_error:
        raise ResponseTimeoutError(f"7709 response timed out during Actor {operation}")
    raise TransportCloseTimeoutError(f"7709 Actor control lock blocked {operation} before deadline")


def _remove_cleanup_error_locked(runtime: ActorRuntime, error: BaseException) -> None:
    deferred = runtime.deferred_cleanup_error
    if runtime.cleanup_error is error:
        runtime.cleanup_error = None if deferred is error else deferred
    if deferred is error or runtime.cleanup_error is deferred:
        runtime.deferred_cleanup_error = None


def close_actor(runtime: ActorRuntime, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + max(0.0, timeout)
    if runtime.stopped.is_set() and runtime.fatal_error is not None:
        _settle_fatal(runtime, deadline=deadline)
    control_token = _acquire_control_lock(runtime, deadline, "close inspection")
    try:
        failed_before_close = runtime.fatal_error is not None or runtime.state in (
            RuntimeState.FAILED,
            RuntimeState.FAILED_CLOSING,
            RuntimeState.FAILED_CLOSED,
        )
    finally:
        runtime.control_lock.release_token(control_token)
    request_actor_stop(runtime, deadline=deadline)
    thread = runtime.actor_thread
    if thread is not None and thread.ident is not None and thread is not threading.current_thread():
        thread.join(max(0.0, deadline - time.monotonic()))
    if thread is not None and thread.is_alive():
        control_token = _acquire_control_lock(runtime, deadline, "failed-close publication")
        try:
            runtime.state = RuntimeState.FAILED_CLOSING
        finally:
            runtime.control_lock.release_token(control_token)
        raise TransportCloseTimeoutError("7709 Actor did not stop within 1 second")
    if runtime.owns_push_buffer and not runtime.owner_settles_push and runtime.push_buffer is not None:
        previous_push_error = runtime.push_cleanup_error
        try:
            runtime.push_buffer.close_before_deadline(deadline, runtime.fatal_error)
        except BaseException as exc:
            runtime.push_cleanup_error = exc
            if runtime.cleanup_error is None or runtime.cleanup_error is previous_push_error:
                runtime.cleanup_error = exc
        else:
            control_token = _acquire_control_lock(runtime, deadline, "push cleanup completion")
            try:
                if previous_push_error is not None and runtime.cleanup_error is previous_push_error:
                    runtime.cleanup_error = runtime.deferred_cleanup_error
                runtime.push_cleanup_error = None
            finally:
                runtime.control_lock.release_token(control_token)
    control_token = _acquire_control_lock(runtime, deadline, "close completion")
    try:
        unreported_fatal_error = runtime.unreported_fatal_settlement_error
        cleanup_error = unreported_fatal_error or runtime.cleanup_error
        if unreported_fatal_error is not None:
            runtime.unreported_fatal_settlement_error = None
            _remove_cleanup_error_locked(runtime, unreported_fatal_error)
        if cleanup_error is not None:
            runtime.state = RuntimeState.FAILED_CLOSING
        elif failed_before_close or runtime.state is RuntimeState.FAILED_CLOSING:
            runtime.state = RuntimeState.FAILED_CLOSED
    finally:
        runtime.control_lock.release_token(control_token)
    if cleanup_error is not None:
        raise TransportCloseTimeoutError("7709 Actor resource cleanup failed") from cleanup_error


def abandon_actor(runtime: ActorRuntime) -> None:
    """Best-effort non-blocking finalizer callback for one exact runtime."""

    runtime.stop_requested = True
    writer = runtime.wake_writer
    if writer is None:
        return
    try:
        _notify_actor(runtime, writer)
    except BaseException:
        pass


def actor_snapshot(runtime: ActorRuntime) -> ActorSnapshot:
    with runtime.control_lock:
        generation = runtime.generation
        thread = runtime.actor_thread
        return ActorSnapshot(
            runtime_epoch=runtime.runtime_epoch,
            state=runtime.state,
            tcp_state=generation.state if generation is not None else TcpState.DOWN,
            tcp_generation=generation.generation_id if generation is not None else runtime.generation_counter,
            connected_host=runtime.connected_host,
            actor_alive=thread is not None and thread.is_alive(),
            pending_depth=int(runtime.pending_task is not None) + int(runtime.active_task is not None),
            reconnect_count=runtime.reconnect_count,
            stale_event_count=runtime.stale_event_count,
            last_error=str(runtime.last_error) if runtime.last_error is not None else None,
        )


def _run_actor(runtime: ActorRuntime) -> None:
    try:
        selector = runtime.selector_factory()
        with runtime.control_lock:
            runtime.selector = selector
        wake_reader, wake_writer = socket.socketpair()
        with runtime.control_lock:
            runtime.wake_reader = wake_reader
            runtime.wake_writer = wake_writer
        wake_reader.setblocking(False)
        wake_writer.setblocking(False)
        selector.register(
            wake_reader,
            selectors.EVENT_READ,
            SelectorToken("wakeup", runtime.runtime_epoch, 0, wake_reader),
        )
        with runtime.control_lock:
            if runtime.stop_requested:
                if runtime.state is not RuntimeState.FAILED_CLOSING:
                    runtime.state = RuntimeState.CLOSING
            else:
                runtime.state = RuntimeState.RUNNING
            runtime.started.set()

        while True:
            _drain_control(runtime)
            if runtime.stop_requested:
                break
            _expire_active_task(runtime)
            generation = runtime.generation
            if generation is not None and generation.decoded_frames:
                _receive_generation_safely(runtime, generation)
                continue
            _expire_active_task(runtime)
            _advance_active_task(runtime)
            _schedule_heartbeat(runtime)
            timeout = _selector_timeout(runtime)
            events = selector.select(timeout)
            wake_seen = False
            for key, _ in events:
                token = key.data
                if isinstance(token, SelectorToken) and token.kind == "wakeup":
                    wake_seen = True
                    _drain_wakeup(runtime)
            _drain_control(runtime)
            if runtime.stop_requested:
                break
            _expire_active_task(runtime)
            tcp_seen = False
            for key, mask in events:
                token = key.data
                if isinstance(token, SelectorToken) and token.kind == "tcp":
                    tcp_seen = True
                    _handle_tcp_event(runtime, token, mask)
            _expire_active_task(runtime)
            _advance_wake_only_batch(runtime, wake_seen=wake_seen, tcp_seen=tcp_seen)
    except BaseException as exc:
        with runtime.control_lock:
            runtime.fatal_error = exc
            runtime.last_error = exc
            runtime.state = RuntimeState.FAILED
        _publish_fatal(runtime, exc)
    finally:
        _finish_runtime(runtime)


def _advance_wake_only_batch(runtime: ActorRuntime, *, wake_seen: bool, tcp_seen: bool) -> None:
    if not wake_seen or tcp_seen:
        return
    generation = runtime.generation
    if generation is not None and (generation.decoded_frames or generation.decoder.buffered_bytes):
        return
    _advance_active_task(runtime)


def _drain_control(runtime: ActorRuntime) -> None:
    while True:
        with runtime.control_lock:
            cancels = tuple(runtime.cancel_requests.values())
            runtime.cancel_requests.clear()
            if not cancels:
                if runtime.stop_requested or runtime.active_task is not None:
                    return
                task = runtime.pending_task
                runtime.pending_task = None
                runtime.active_task = task
                generation = runtime.generation
                if (
                    isinstance(task, RequestTicket)
                    and generation is not None
                    and generation.active_exchange is None
                ):
                    generation.receive_drained = False
                return
        for cancel in cancels:
            _apply_cancel(runtime, cancel)


def _advance_active_task(runtime: ActorRuntime) -> None:
    task = runtime.active_task
    if isinstance(task, ConnectTicket):
        generation = runtime.generation
        if generation is not None and generation.state in (TcpState.CONNECTED_UNHANDSHAKEN, TcpState.READY):
            if _claim_active_ticket_terminal(runtime, task, generation=generation):
                _complete_ticket(
                    task,
                    RequestState.SUCCESS,
                    connected_host=generation.endpoint.host,
                    terminal_claimed=True,
                )
                if runtime.active_task is task:
                    runtime.active_task = None
        elif generation is None:
            runtime.endpoint_index = 0
            runtime.endpoints_remaining = len(runtime.endpoints)
            _start_next_endpoint(runtime)
        return
    if not isinstance(task, RequestTicket):
        return

    generation = runtime.generation
    if generation is None:
        _start_request_attempt(runtime)
        return
    if generation.active_exchange is not None:
        if (
            generation.tx_offset < len(generation.tx_bytes)
            and generation.selector_events == selectors.EVENT_READ
            and not generation.decoded_frames
            and not generation.decoder.buffered_bytes
        ):
            try:
                _send_generation(runtime, generation)
            except (OSError, ConnectionClosedError) as exc:
                _handle_wire_failure(runtime, exc, retryable=True)
        return
    if generation.state is TcpState.CONNECTING:
        return
    if generation.state not in (TcpState.CONNECTED_UNHANDSHAKEN, TcpState.READY):
        return
    if not generation.receive_drained or generation.decoder.buffered_bytes:
        drained = _receive_generation_safely(runtime, generation)
        if runtime.generation is not generation or not drained:
            return
        if generation.decoded_frames or generation.decoder.buffered_bytes:
            return
    if task.attempts == 0:
        _start_request_attempt(runtime)
    elif generation.state is TcpState.CONNECTED_UNHANDSHAKEN:
        _begin_exchange(
            runtime,
            task,
            TYPE_HANDSHAKE,
            handshake=task.command != TYPE_HANDSHAKE,
        )
    else:
        _begin_exchange(runtime, task, task.command, handshake=False)


def _apply_cancel(runtime: ActorRuntime, cancel: CancelToken) -> None:
    if cancel.runtime_epoch != runtime.runtime_epoch:
        return
    ticket = runtime.active_task
    if isinstance(ticket, (ConnectTicket, RequestTicket)) and (
        ticket.request_id == cancel.request_id and ticket.lease_id == cancel.lease_id
    ):
        generation = runtime.generation
        if isinstance(ticket, ConnectTicket):
            if generation is not None and generation.state is TcpState.CONNECTING:
                _drop_generation(runtime, ConnectionClosedError("7709 connect cancelled"))
        else:
            exchange = generation.active_exchange if generation is not None else None
            if generation is not None and generation.state is TcpState.CONNECTING:
                _drop_generation(runtime, ConnectionClosedError("7709 request cancelled during connect"))
            elif exchange is not None and exchange.ticket is ticket:
                if exchange.sent_any:
                    _drop_generation(runtime, ConnectionClosedError("7709 request cancelled after send"))
                else:
                    generation.active_exchange = None
                    generation.tx_bytes = b""
                    generation.tx_offset = 0
                    generation.state = TcpState.CONNECTED_UNHANDSHAKEN if exchange.handshake else TcpState.READY
                    _set_generation_interest(runtime, generation, selectors.EVENT_READ)
        _complete_ticket(ticket, RequestState.CANCELLED, error=ConnectionClosedError("7709 request cancelled"))
        runtime.active_task = None
        return
    with runtime.control_lock:
        pending = runtime.pending_task
        if isinstance(pending, (ConnectTicket, RequestTicket)) and (
            pending.request_id == cancel.request_id and pending.lease_id == cancel.lease_id
        ):
            _claim_ticket_terminal(pending)
            runtime.pending_task = None
        else:
            pending = None
    if pending is not None:
        _complete_ticket(
            pending,
            RequestState.CANCELLED,
            error=ConnectionClosedError("7709 request cancelled"),
            terminal_claimed=True,
        )


def _exact_cancel_for_ticket(
    runtime: ActorRuntime,
    ticket: ConnectTicket | RequestTicket,
) -> CancelToken | None:
    cancel = runtime.cancel_requests.get(ticket.request_id)
    if cancel is None:
        return None
    if (
        cancel.runtime_epoch != runtime.runtime_epoch
        or cancel.request_id != ticket.request_id
        or cancel.lease_id != ticket.lease_id
    ):
        return None
    return cancel


def _start_request_attempt(runtime: ActorRuntime) -> None:
    ticket = runtime.active_task
    if not isinstance(ticket, RequestTicket):
        return
    if time.monotonic() >= ticket.deadline:
        _fail_active_task(
            runtime,
            ResponseTimeoutError("7709 response timed out before connect"),
            retryable=False,
        )
        return
    ticket.attempts += 1
    attempts_remaining = 1 if ticket.internal else max(1, _MAX_REQUEST_ATTEMPTS - ticket.attempts + 1)
    now = time.monotonic()
    ticket.attempt_deadline = now + max(0.0, ticket.deadline - now) / attempts_remaining
    generation = runtime.generation
    if generation is None:
        runtime.endpoint_index = ticket.next_endpoint_index % max(1, len(runtime.endpoints))
        runtime.endpoints_remaining = len(runtime.endpoints)
        _start_next_endpoint(runtime)
        return
    if generation.state is TcpState.CONNECTED_UNHANDSHAKEN and ticket.command != TYPE_HANDSHAKE:
        _begin_exchange(runtime, ticket, TYPE_HANDSHAKE, handshake=True)
        return
    _begin_exchange(runtime, ticket, ticket.command, handshake=False)


def _begin_exchange(runtime: ActorRuntime, ticket: RequestTicket, command: int, *, handshake: bool) -> bool:
    generation = runtime.generation
    if generation is None:
        raise RuntimeError("cannot start wire exchange without a TCP generation")
    next_msg_id = _next_message_id(runtime)
    payload = {} if handshake else ticket.request_payload_snapshot
    try:
        if not isinstance(payload, dict):
            payload = dict(payload)  # type: ignore[arg-type]
        request = build_command_frame(command, payload, next_msg_id)
        frame = request.to_bytes()
    except Exception as exc:
        _fail_active_task(runtime, exc, retryable=False)
        return False
    generation.exchange_counter += 1
    generation.tx_bytes = frame
    generation.tx_offset = 0
    generation.active_exchange = WireExchange(
        ticket=ticket,
        command=command,
        msg_id=request.msg_id,
        msg_type=request.msg_type,
        frame=frame,
        handshake=handshake,
        rx_boundary=generation.rx_sequence,
        exchange_id=generation.exchange_counter,
    )
    generation.state = TcpState.HANDSHAKING if handshake else TcpState.READY
    if not handshake:
        ticket.state = RequestState.SENDING
    try:
        _send_generation(runtime, generation)
    except (OSError, ConnectionClosedError) as exc:
        _handle_wire_failure(runtime, exc, retryable=True)
    return True


def _start_next_endpoint(runtime: ActorRuntime) -> None:
    ticket = runtime.active_task
    if not isinstance(ticket, (ConnectTicket, RequestTicket)):
        return
    while runtime.endpoints and runtime.endpoints_remaining > 0:
        if not _claim_generation_start(runtime, ticket):
            return
        now = time.monotonic()
        budget_deadline = _attempt_budget_deadline(ticket)
        if now >= budget_deadline:
            _fail_active_task(
                runtime,
                ResponseTimeoutError("7709 response timed out during connect"),
                retryable=isinstance(ticket, RequestTicket) and budget_deadline < ticket.deadline,
            )
            return
        candidate_count = runtime.endpoints_remaining
        endpoint_index = runtime.endpoint_index % len(runtime.endpoints)
        endpoint = runtime.endpoints[endpoint_index]
        runtime.endpoint_index = (endpoint_index + 1) % len(runtime.endpoints)
        runtime.endpoints_remaining -= 1
        connect_deadline = now + max(0.0, budget_deadline - now) / candidate_count
        try:
            sock = runtime.socket_factory(endpoint.family, endpoint.socktype, endpoint.proto)
        except OSError as exc:
            runtime.last_error = exc
            continue
        runtime.generation_counter += 1
        generation = TcpGeneration(
            generation_id=runtime.generation_counter,
            sock=sock,
            endpoint=endpoint,
            state=TcpState.CONNECTING,
            endpoint_index=endpoint_index,
            connect_deadline=connect_deadline,
            last_activity_at=time.monotonic(),
        )
        runtime.generation = generation
        runtime.generation_started.set()
        try:
            sock.setblocking(False)
        except OSError as exc:
            runtime.last_error = exc
            _drop_generation(runtime, exc)
            continue
        try:
            result = sock.connect_ex(endpoint.sockaddr)
        except OSError as exc:
            runtime.last_error = exc
            _drop_generation(runtime, exc)
            continue
        if result in _SUCCESS_CONNECT_CODES:
            _finish_connect(runtime, generation)
            return
        if result in _IN_PROGRESS_CONNECT_CODES:
            _register_connecting(runtime, generation)
            return
        error = OSError(result, f"connect_ex failed for {endpoint.host}")
        runtime.last_error = error
        _drop_generation(runtime, error)
    error = ConnectionClosedError("unable to connect to any 7709 host")
    if runtime.last_error is not None:
        error.__cause__ = runtime.last_error
    _fail_active_task(runtime, error, retryable=True)


def _claim_generation_start(runtime: ActorRuntime, ticket: ConnectTicket | RequestTicket) -> bool:
    with runtime.control_lock:
        claimed = (
            not runtime.stop_requested
            and runtime.active_task is ticket
            and _exact_cancel_for_ticket(runtime, ticket) is None
        )
    if not claimed:
        _drain_control(runtime)
    return claimed


def _next_message_id(runtime: ActorRuntime) -> int:
    while runtime.msg_id_counter < 0xFFFFFFFF:
        runtime.msg_id_counter += 1
        message_id = _message_id_for_counter(runtime, runtime.msg_id_counter)
        if message_id != 0:
            return message_id
    raise TransportError("7709 message identity space exhausted")


def _message_id_for_counter(runtime: ActorRuntime, value: int) -> int:
    left = (value >> 16) & 0xFFFF
    right = value & 0xFFFF
    for round_index in range(4):
        digest = hashlib.blake2s(
            right.to_bytes(2, "little") + bytes((round_index,)),
            key=runtime.msg_id_key,
            digest_size=2,
        ).digest()
        left, right = right, left ^ int.from_bytes(digest, "little")
    return ((left & 0xFFFF) << 16) | (right & 0xFFFF)


def _attempt_budget_deadline(ticket: ConnectTicket | RequestTicket) -> float:
    if isinstance(ticket, RequestTicket) and ticket.attempt_deadline > 0:
        return min(ticket.deadline, ticket.attempt_deadline)
    return ticket.deadline


def _register_connecting(runtime: ActorRuntime, generation: TcpGeneration) -> None:
    selector = runtime.selector
    if selector is None:
        raise RuntimeError("Actor selector is unavailable")
    selector.register(
        generation.sock,
        selectors.EVENT_READ | selectors.EVENT_WRITE,
        SelectorToken("tcp", runtime.runtime_epoch, generation.generation_id, generation.sock),
    )
    generation.selector_events = selectors.EVENT_READ | selectors.EVENT_WRITE


def _set_generation_interest(runtime: ActorRuntime, generation: TcpGeneration, events: int) -> None:
    if generation.selector_events == events:
        return
    selector = runtime.selector
    if selector is None:
        raise RuntimeError("Actor selector is unavailable")
    token = SelectorToken("tcp", runtime.runtime_epoch, generation.generation_id, generation.sock)
    try:
        selector.modify(generation.sock, events, token)
    except KeyError:
        selector.register(generation.sock, events, token)
    generation.selector_events = events


def _handle_tcp_event(runtime: ActorRuntime, token: SelectorToken, mask: int) -> None:
    generation = runtime.generation
    if (
        token.runtime_epoch != runtime.runtime_epoch
        or generation is None
        or token.tcp_generation != generation.generation_id
        or token.sock is not generation.sock
    ):
        runtime.stale_event_count += 1
        return
    if generation.state is not TcpState.CONNECTING:
        if runtime.generation is generation and mask & selectors.EVENT_READ:
            generation.receive_drained = False
            drained = _receive_generation_safely(runtime, generation)
            if runtime.generation is not generation:
                return
            if generation.decoder.buffered_bytes:
                _set_generation_interest(runtime, generation, selectors.EVENT_READ)
                return
            if (
                not drained
                or generation.decoded_frames
            ):
                return
        if generation.decoded_frames or generation.decoder.buffered_bytes:
            return
        try:
            exchange = generation.active_exchange
            if mask & selectors.EVENT_WRITE or (
                mask & selectors.EVENT_READ
                and exchange is not None
                and generation.tx_offset < len(generation.tx_bytes)
            ):
                _send_generation(runtime, generation)
        except (OSError, ConnectionClosedError) as exc:
            _handle_wire_failure(runtime, exc, retryable=True)
            return
        return
    try:
        error_code = generation.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    except (OSError, ConnectionClosedError) as exc:
        runtime.last_error = exc
        _drop_generation(runtime, exc)
        _start_next_endpoint(runtime)
        return
    if error_code == 0:
        getpeername = getattr(generation.sock, "getpeername", None)
        if getpeername is not None:
            try:
                getpeername()
            except OSError as exc:
                runtime.last_error = exc
                _drop_generation(runtime, exc)
                _start_next_endpoint(runtime)
                return
        _finish_connect(runtime, generation)
        return
    if error_code in _IN_PROGRESS_CONNECT_CODES:
        _defer_connect_probe(runtime, generation)
        return
    error = OSError(error_code, f"connect failed for {generation.endpoint.host}")
    runtime.last_error = error
    _drop_generation(runtime, error)
    _start_next_endpoint(runtime)


def _defer_connect_probe(runtime: ActorRuntime, generation: TcpGeneration) -> None:
    selector = runtime.selector
    if selector is None:
        raise RuntimeError("Actor selector is unavailable")
    try:
        selector.unregister(generation.sock)
    except (KeyError, ValueError):
        pass
    generation.selector_events = 0
    generation.connect_recheck_at = min(
        generation.connect_deadline,
        time.monotonic() + _CONNECT_RECHECK_INTERVAL,
    )


def _send_generation(runtime: ActorRuntime, generation: TcpGeneration) -> None:
    exchange = generation.active_exchange
    if exchange is None or generation.tx_offset >= len(generation.tx_bytes):
        return
    _drain_control(runtime)
    if runtime.stop_requested:
        return
    _expire_active_task(runtime)
    if (
        runtime.generation is not generation
        or runtime.active_task is not exchange.ticket
        or generation.active_exchange is not exchange
    ):
        return
    generation.receive_drained = False
    drained = _receive_generation_safely(runtime, generation)
    if (
        runtime.generation is not generation
        or runtime.active_task is not exchange.ticket
        or generation.active_exchange is not exchange
    ):
        return
    if generation.decoder.buffered_bytes:
        _set_generation_interest(runtime, generation, selectors.EVENT_READ)
        return
    if not drained or generation.decoded_frames:
        return
    _drain_control(runtime)
    if runtime.stop_requested:
        return
    _expire_active_task(runtime)
    if (
        runtime.generation is not generation
        or runtime.active_task is not exchange.ticket
        or generation.active_exchange is not exchange
    ):
        return
    if not _claim_generation_send(runtime, generation, exchange):
        return
    try:
        sent = generation.sock.send(memoryview(generation.tx_bytes)[generation.tx_offset:])
    except BlockingIOError:
        _set_generation_interest(runtime, generation, selectors.EVENT_READ | selectors.EVENT_WRITE)
        return
    if sent == 0:
        raise ConnectionClosedError("7709 socket closed during send")
    exchange.sent_any = True
    if not exchange.handshake and not exchange.ticket.retry_safe:
        exchange.ticket.attempt_deadline = exchange.ticket.deadline
    generation.tx_offset += sent
    generation.last_activity_at = time.monotonic()
    if generation.tx_offset < len(generation.tx_bytes):
        _set_generation_interest(runtime, generation, selectors.EVENT_READ | selectors.EVENT_WRITE)
        return
    exchange.rx_boundary = generation.rx_sequence
    if not exchange.handshake:
        exchange.ticket.state = RequestState.WAITING_RESPONSE
    _set_generation_interest(runtime, generation, selectors.EVENT_READ)


def _claim_generation_send(
    runtime: ActorRuntime,
    generation: TcpGeneration,
    exchange: WireExchange,
) -> bool:
    with runtime.control_lock:
        claimed = (
            not runtime.stop_requested
            and _exact_cancel_for_ticket(runtime, exchange.ticket) is None
            and time.monotonic() < _attempt_budget_deadline(exchange.ticket)
            and runtime.generation is generation
            and runtime.active_task is exchange.ticket
            and generation.active_exchange is exchange
        )
        if claimed:
            exchange.send_claimed = True
    if not claimed:
        _drain_control(runtime)
        _expire_active_task(runtime)
    return claimed


def _receive_generation_safely(runtime: ActorRuntime, generation: TcpGeneration) -> bool:
    try:
        return _receive_generation(runtime, generation)
    except ProtocolError as exc:
        _handle_wire_failure(runtime, exc, retryable=False)
    except (OSError, ConnectionClosedError) as exc:
        _handle_wire_failure(runtime, exc, retryable=True)
    return True


def _receive_generation(runtime: ActorRuntime, generation: TcpGeneration) -> bool:
    bytes_read = 0
    frames_read = 0
    while bytes_read < 256 * 1024 and frames_read < 64:
        while generation.decoded_frames and frames_read < 64:
            received = generation.decoded_frames.popleft()
            frames_read += 1
            _route_frame(runtime, generation, received)
            if runtime.generation is not generation:
                return True
        if generation.decoded_frames:
            return False
        try:
            chunk = generation.sock.recv(min(_MAX_RECEIVE_CHUNK, 256 * 1024 - bytes_read))
        except BlockingIOError:
            generation.receive_drained = True
            return True
        if not chunk:
            try:
                generation.decoder.finish()
            except ProtocolError as exc:
                raise ConnectionClosedError("7709 socket closed with a partial response frame") from exc
            raise ConnectionClosedError("7709 socket closed by remote peer")
        generation.receive_drained = False
        bytes_read += len(chunk)
        generation.last_activity_at = time.monotonic()
        frames = generation.decoder.feed(chunk)
        if len(generation.decoded_frames) + len(frames) > _MAX_DECODED_FRAMES:
            raise ProtocolError(
                f"decoded response frame queue exceeds limit: {_MAX_DECODED_FRAMES}"
            )
        exchange = generation.active_exchange
        exchange_id = exchange.exchange_id if exchange is not None else None
        send_complete = bool(
            exchange is not None
            and generation.tx_offset >= len(generation.tx_bytes)
        )
        for response in frames:
            generation.rx_sequence += 1
            generation.decoded_frames.append(
                ReceivedFrame(
                    generation.rx_sequence,
                    response,
                    exchange_id=exchange_id,
                    send_complete=send_complete,
                )
            )
    return False


def _route_frame(runtime: ActorRuntime, generation: TcpGeneration, received: ReceivedFrame) -> None:
    response = received.response
    exchange = generation.active_exchange
    active = runtime.active_task
    if active is not None and time.monotonic() >= _attempt_budget_deadline(active):
        _expire_active_task(runtime)
        return
    if (
        exchange is None
        or exchange.ticket is not active
        or exchange.ticket.runtime_epoch != runtime.runtime_epoch
        or received.exchange_id != exchange.exchange_id
        or not received.send_complete
        or received.sequence <= exchange.rx_boundary
        or generation.tx_offset < len(generation.tx_bytes)
        or response.msg_id != exchange.msg_id
        or response.msg_type != exchange.msg_type
    ):
        push_buffer = runtime.push_buffer
        if push_buffer is None:
            runtime.stale_event_count += 1
        else:
            push_buffer.offer_nowait(
                PushFrame(runtime.runtime_epoch, generation.generation_id, generation.endpoint.host, response),
                drop_publication=runtime.push_drop_publication,
            )
        return

    ticket = exchange.ticket
    handshake_result: object | None = None
    heartbeat_result: object | None = None
    if exchange.handshake or ticket.command == TYPE_HANDSHAKE:
        try:
            handshake_result = parse_command_response(TYPE_HANDSHAKE, response, {})
        except ProtocolError as exc:
            _handle_wire_failure(runtime, exc, retryable=True)
            return
    elif ticket.command == TYPE_HEARTBEAT:
        heartbeat_result = parse_command_response(TYPE_HEARTBEAT, response, {})
    terminal_response = not exchange.handshake or ticket.command == TYPE_HANDSHAKE
    if terminal_response:
        if not _claim_active_ticket_terminal(runtime, ticket, generation=generation, exchange=exchange):
            return
    elif not _control_allows_active_ticket(runtime, ticket, generation=generation, exchange=exchange):
        return
    if exchange.handshake:
        runtime.last_handshake = handshake_result
        generation.active_exchange = None
        generation.tx_bytes = b""
        generation.tx_offset = 0
        generation.state = TcpState.READY
        if ticket.command != TYPE_HANDSHAKE:
            ticket.state = RequestState.ADMITTED
            _set_generation_interest(runtime, generation, selectors.EVENT_READ)
            return
    elif ticket.command == TYPE_HANDSHAKE:
        runtime.last_handshake = handshake_result
    elif ticket.command == TYPE_HEARTBEAT:
        runtime.last_heartbeat = heartbeat_result
    envelope = FrameEnvelope(
        runtime_epoch=runtime.runtime_epoch,
        tcp_generation=generation.generation_id,
        lease_id=ticket.lease_id,
        request_id=ticket.request_id,
        msg_id=response.msg_id,
        msg_type=response.msg_type,
        command=ticket.command,
        connected_host=generation.endpoint.host,
        request_payload_snapshot=ticket.request_payload_snapshot,
        response=response,
    )
    generation.active_exchange = None
    generation.tx_bytes = b""
    generation.tx_offset = 0
    generation.state = TcpState.READY
    _complete_ticket(ticket, RequestState.SUCCESS, result=envelope, terminal_claimed=True)
    if runtime.active_task is ticket:
        runtime.active_task = None
    _wait_for_successor(runtime, ticket)


def _control_allows_active_ticket(
    runtime: ActorRuntime,
    ticket: ConnectTicket | RequestTicket,
    *,
    generation: TcpGeneration | None = None,
    exchange: WireExchange | None = None,
) -> bool:
    with runtime.control_lock:
        allowed = (
            not runtime.stop_requested
            and runtime.active_task is ticket
            and _exact_cancel_for_ticket(runtime, ticket) is None
            and (generation is None or runtime.generation is generation)
            and (exchange is None or (generation is not None and generation.active_exchange is exchange))
        )
    if not allowed:
        _drain_control(runtime)
    return allowed


def _claim_active_ticket_terminal(
    runtime: ActorRuntime,
    ticket: ConnectTicket | RequestTicket,
    *,
    generation: TcpGeneration | None = None,
    exchange: WireExchange | None = None,
) -> bool:
    with runtime.control_lock:
        claimed = (
            not runtime.stop_requested
            and runtime.active_task is ticket
            and _exact_cancel_for_ticket(runtime, ticket) is None
            and (generation is None or runtime.generation is generation)
            and (exchange is None or (generation is not None and generation.active_exchange is exchange))
            and _claim_ticket_terminal(ticket)
        )
    if not claimed:
        _drain_control(runtime)
    return claimed


def _wait_for_successor(runtime: ActorRuntime, ticket: RequestTicket) -> None:
    if ticket.internal:
        return
    terminal_yield = runtime.terminal_yield
    successor_grace = runtime.successor_grace
    if not terminal_yield and successor_grace <= 0:
        return
    with runtime.control_lock:
        if runtime.stop_requested or runtime.pending_task is not None or runtime.cancel_requests:
            return
        if successor_grace > 0:
            runtime.control_ready.clear()
            if runtime.stop_requested:
                return
    if terminal_yield:
        time.sleep(0)
    else:
        runtime.control_ready.wait(successor_grace)


def _handle_wire_failure(runtime: ActorRuntime, error: BaseException, *, retryable: bool) -> None:
    generation = runtime.generation
    exchange = generation.active_exchange if generation is not None else None
    sent_business = bool(exchange is not None and not exchange.handshake and exchange.sent_any)
    ticket = runtime.active_task
    if isinstance(ticket, RequestTicket) and generation is not None and runtime.endpoints:
        ticket.next_endpoint_index = (generation.endpoint_index + 1) % len(runtime.endpoints)
    _drop_generation(runtime, error)
    _fail_active_task(runtime, error, retryable=retryable, sent_business=sent_business)


def _finish_connect(runtime: ActorRuntime, generation: TcpGeneration) -> None:
    selector = runtime.selector
    if selector is not None:
        try:
            selector.unregister(generation.sock)
        except (KeyError, ValueError):
            pass
    generation.selector_events = 0
    now = time.monotonic()
    generation.state = TcpState.CONNECTED_UNHANDSHAKEN
    generation.connected_at = now
    generation.last_activity_at = now
    runtime.connected_host = generation.endpoint.host
    ticket = runtime.active_task
    if isinstance(ticket, ConnectTicket):
        if _claim_active_ticket_terminal(runtime, ticket, generation=generation):
            _complete_ticket(
                ticket,
                RequestState.SUCCESS,
                connected_host=generation.endpoint.host,
                terminal_claimed=True,
            )
            if runtime.active_task is ticket:
                runtime.active_task = None
    elif isinstance(ticket, RequestTicket):
        generation.receive_drained = False
        _set_generation_interest(runtime, generation, selectors.EVENT_READ)


def _expire_active_task(runtime: ActorRuntime) -> None:
    ticket = runtime.active_task
    if ticket is None:
        return
    now = time.monotonic()
    generation = runtime.generation
    if (
        generation is not None
        and generation.state is TcpState.CONNECTING
        and generation.connect_deadline > 0
        and now >= generation.connect_deadline
    ):
        error = ResponseTimeoutError("7709 response timed out during connect")
        _drop_generation(runtime, error)
        if now < ticket.deadline:
            _start_next_endpoint(runtime)
        else:
            _fail_active_task(runtime, error, retryable=False)
        return
    if (
        generation is not None
        and generation.state is TcpState.CONNECTING
        and generation.connect_recheck_at > 0
        and now >= generation.connect_recheck_at
    ):
        generation.connect_recheck_at = 0.0
        _register_connecting(runtime, generation)
        return

    expiry_deadline = _attempt_budget_deadline(ticket)
    if now < expiry_deadline:
        return
    exchange = generation.active_exchange if generation is not None else None
    if generation is None or generation.state is TcpState.CONNECTING:
        stage = "connect"
    elif exchange is not None and exchange.handshake:
        stage = "handshake"
    elif exchange is not None and generation.tx_offset < len(generation.tx_bytes):
        stage = "send"
    else:
        stage = "response"
    error = ResponseTimeoutError(f"7709 response timed out during {stage}")
    sent_business = bool(exchange is not None and not exchange.handshake and exchange.sent_any)
    if isinstance(ticket, RequestTicket) and generation is not None and runtime.endpoints:
        ticket.next_endpoint_index = (generation.endpoint_index + 1) % len(runtime.endpoints)
    _drop_generation(runtime, error)
    _fail_active_task(
        runtime,
        error,
        retryable=isinstance(ticket, RequestTicket) and expiry_deadline < ticket.deadline,
        sent_business=sent_business,
    )


def _selector_timeout(runtime: ActorRuntime) -> float | None:
    ticket = runtime.active_task
    now = time.monotonic()
    if ticket is not None:
        deadlines = [ticket.deadline, _attempt_budget_deadline(ticket)]
        generation = runtime.generation
        if generation is not None and generation.state is TcpState.CONNECTING and generation.connect_deadline > 0:
            deadlines.append(generation.connect_deadline)
            if generation.connect_recheck_at > 0:
                deadlines.append(generation.connect_recheck_at)
        return max(0.0, min(deadlines) - now)
    generation = runtime.generation
    interval = runtime.heartbeat_interval
    if generation is not None and interval is not None and interval > 0:
        return max(0.0, generation.last_activity_at + interval - now)
    return None


def _schedule_heartbeat(runtime: ActorRuntime) -> None:
    interval = runtime.heartbeat_interval
    generation = runtime.generation
    if interval is None or interval <= 0 or generation is None:
        return
    with runtime.control_lock:
        if (
            runtime.stop_requested
            or runtime.active_task is not None
            or runtime.pending_task is not None
            or runtime.cancel_requests
            or runtime.generation is not generation
            or runtime.heartbeat_interval != interval
        ):
            return
    now = time.monotonic()
    if now < generation.last_activity_at + interval:
        return
    heartbeat_allowed = runtime.heartbeat_allowed
    try_allowed = getattr(heartbeat_allowed, "try_allowed", None)
    allowed = heartbeat_allowed is None or (try_allowed is not None and try_allowed())
    with runtime.control_lock:
        if runtime.generation is not generation or runtime.heartbeat_interval != interval:
            return
        if not allowed:
            generation.last_activity_at = max(generation.last_activity_at, now)
            return
        claim_now = time.monotonic()
        if (
            runtime.stop_requested
            or runtime.active_task is not None
            or runtime.pending_task is not None
            or runtime.cancel_requests
            or claim_now < generation.last_activity_at + interval
        ):
            return
        runtime.request_id_counter += 1
        ticket = RequestTicket(
            runtime_epoch=runtime.runtime_epoch,
            lease_id=-1,
            command=TYPE_HEARTBEAT,
            request_payload_snapshot={},
            deadline=claim_now + runtime.request_timeout,
            retry_safe=False,
            request_id=runtime.request_id_counter,
            internal=True,
        )
        runtime.active_task = ticket
    _advance_active_task(runtime)


def _fail_active_task(
    runtime: ActorRuntime,
    error: BaseException,
    *,
    retryable: bool,
    sent_business: bool = False,
) -> None:
    ticket = runtime.active_task
    if not isinstance(ticket, (ConnectTicket, RequestTicket)):
        return
    if isinstance(ticket, RequestTicket):
        can_retry = (
            retryable
            and not ticket.internal
            and ticket.attempts < _MAX_REQUEST_ATTEMPTS
            and time.monotonic() < ticket.deadline
            and (ticket.retry_safe or not sent_business)
        )
        if can_retry:
            if not _control_allows_active_ticket(runtime, ticket):
                return
            ticket.state = RequestState.ADMITTED
            _start_request_attempt(runtime)
            return
    if _claim_active_ticket_terminal(runtime, ticket):
        _complete_ticket(ticket, RequestState.FAILED, error=error, terminal_claimed=True)
        if runtime.active_task is ticket:
            runtime.active_task = None


def _complete_ticket(
    ticket: ConnectTicket | RequestTicket,
    state: RequestState,
    *,
    connected_host: str | None = None,
    result: object | None = None,
    error: BaseException | None = None,
    terminal_claimed: bool = False,
) -> bool:
    with ticket.lock:
        if ticket.state in TERMINAL_REQUEST_STATES:
            return False
        if terminal_claimed:
            if not ticket.terminal_claimed:
                return False
        else:
            if ticket.terminal_claimed:
                return False
            ticket.terminal_claimed = True
        ticket.state = state
        ticket.error = error
        ticket.completed_at = time.monotonic()
        if isinstance(ticket, ConnectTicket):
            ticket.connected_host = connected_host
        else:
            ticket.result = result
    completion = ticket.completion
    publish = getattr(completion, "publish_nonblocking", None)
    if publish is not None:
        try:
            publish(ticket)
        except Exception as exc:
            with ticket.lock:
                ticket.completion_error = exc
    ticket.completed.set()
    return True


def _settle_ticket_completion(ticket: ConnectTicket | RequestTicket) -> None:
    with ticket.lock:
        if ticket.completion_settled:
            return
        ticket.completion_settled = True
        completion = ticket.completion
    if completion is None:
        return
    settle = getattr(completion, "settle", None)
    callback = settle if settle is not None else completion
    try:
        callback(ticket)
    except Exception as exc:
        with ticket.lock:
            if ticket.completion_error is None:
                ticket.completion_error = exc


def _publish_fatal(runtime: ActorRuntime, error: BaseException) -> None:
    callback = runtime.fatal_callback
    if callback is None:
        return
    publish = getattr(callback, "publish_nonblocking", None)
    if publish is None:
        return
    try:
        publish(runtime, error)
    except BaseException as exc:
        with runtime.control_lock:
            if runtime.cleanup_error is None:
                runtime.cleanup_error = exc
            elif runtime.cleanup_error is runtime.push_cleanup_error and runtime.deferred_cleanup_error is None:
                runtime.deferred_cleanup_error = exc


def _settle_fatal(runtime: ActorRuntime, *, deadline: float | None = None) -> None:
    timeout = None if deadline is None else max(0.0, deadline - time.monotonic())
    acquired = (
        runtime.fatal_settlement_lock.acquire()
        if timeout is None
        else runtime.fatal_settlement_lock.acquire(timeout=timeout)
    )
    if not acquired:
        raise TransportCloseTimeoutError("7709 fatal settlement blocked before deadline")
    try:
        _settle_fatal_owned(runtime, deadline=deadline)
    finally:
        runtime.fatal_settlement_lock.release()


def _settle_fatal_owned(runtime: ActorRuntime, *, deadline: float | None = None) -> None:
    callback = runtime.fatal_callback
    with runtime.control_lock:
        if runtime.fatal_settled:
            return
        error = runtime.fatal_error
    if callback is None or error is None:
        with runtime.control_lock:
            runtime.fatal_settled = True
        return
    settle = getattr(callback, "settle", None)
    owned = settle if settle is not None else callback
    try:
        owned(runtime, deadline=deadline) if settle is not None else owned(runtime, error)
    except BaseException as exc:
        with runtime.control_lock:
            previous = runtime.fatal_settlement_error
            legacy_owner_cleanup = settle is None and runtime.owner_settles_push
            if not legacy_owner_cleanup and (
                runtime.cleanup_error is None or runtime.cleanup_error is previous
            ):
                runtime.cleanup_error = exc
            if not legacy_owner_cleanup:
                runtime.fatal_settlement_error = exc
            if (
                not legacy_owner_cleanup
                and not isinstance(exc, (TransportCloseTimeoutError, ResponseTimeoutError))
                and runtime.unreported_fatal_settlement_error is None
            ):
                runtime.unreported_fatal_settlement_error = exc
            if settle is None:
                runtime.fatal_settled = True
        return
    with runtime.control_lock:
        previous = runtime.fatal_settlement_error
        runtime.fatal_settlement_error = None
        runtime.fatal_settled = True
        if (
            previous is not None
            and runtime.unreported_fatal_settlement_error is not previous
        ):
            _remove_cleanup_error_locked(runtime, previous)


def _drop_generation(runtime: ActorRuntime, reason: BaseException | None) -> None:
    generation = runtime.generation
    if generation is None:
        return
    generation.state = TcpState.RETIRING
    selector = runtime.selector
    unregister_error: BaseException | None = None
    unregistered = selector is None
    if selector is not None:
        try:
            selector.unregister(generation.sock)
        except (KeyError, ValueError):
            unregistered = True
        except BaseException as exc:
            unregister_error = exc
        else:
            unregistered = True
    if unregistered:
        generation.selector_events = 0
    close_error: BaseException | None = None
    try:
        generation.sock.close()
    except BaseException as exc:
        close_error = exc
    if close_error is None:
        runtime.generation = None
        runtime.connected_host = None
        runtime.reconnect_count += 1
        if reason is not None:
            runtime.last_error = reason
    if close_error is not None:
        raise close_error
    if unregister_error is not None:
        raise unregister_error


def _drain_wakeup(runtime: ActorRuntime) -> None:
    reader = runtime.wake_reader
    if reader is None:
        return
    while True:
        try:
            data = reader.recv(4096)
        except BlockingIOError:
            return
        if not data:
            if runtime.stop_requested:
                return
            raise ConnectionClosedError("7709 Actor wakeup writer closed")


def _notify_actor(runtime: ActorRuntime, writer: socket.socket | None = None) -> None:
    if runtime.successor_grace > 0:
        runtime.control_ready.set()
    if writer is None:
        with runtime.control_lock:
            writer = runtime.wake_writer
    if writer is None:
        return
    try:
        writer.send(b"\x01")
    except BlockingIOError:
        return
    except OSError:
        if not runtime.stop_requested:
            raise


def _notify_submitted_ticket(
    runtime: ActorRuntime,
    ticket: ConnectTicket | RequestTicket,
    writer: socket.socket | None,
) -> None:
    try:
        _notify_actor(runtime, writer)
    except BaseException as exc:
        with runtime.control_lock:
            withdrawn = runtime.pending_task is ticket
            if withdrawn:
                _claim_ticket_terminal(ticket)
                runtime.pending_task = None
        if withdrawn:
            _complete_ticket(ticket, RequestState.FAILED, error=exc, terminal_claimed=True)


def _fail_actor_startup(
    runtime: ActorRuntime,
    error: BaseException,
    *,
    deadline: float | None = None,
) -> None:
    cleanup_errors: list[BaseException] = []
    push_cleanup_error: BaseException | None = None
    with runtime.control_lock:
        runtime.fatal_error = error
        runtime.last_error = error
        runtime.state = RuntimeState.FAILED
    if runtime.push_buffer is not None and runtime.owns_push_buffer:
        try:
            if deadline is None:
                runtime.push_buffer.close(error)
            else:
                runtime.push_buffer.close_before_deadline(deadline, error)
        except BaseException as exc:
            cleanup_errors.append(exc)
            push_cleanup_error = exc
    callback = runtime.fatal_callback
    if callback is not None:
        callback_succeeded = False
        publish = getattr(callback, "publish_nonblocking", None)
        settle = getattr(callback, "settle", None)
        if publish is not None and settle is not None:
            try:
                publish(runtime, error)
            except BaseException as exc:
                cleanup_errors.append(exc)
            else:
                try:
                    settle(runtime, deadline=deadline)
                except BaseException as exc:
                    cleanup_errors.append(exc)
                    with runtime.control_lock:
                        runtime.fatal_settlement_error = exc
                        if (
                            not isinstance(exc, (TransportCloseTimeoutError, ResponseTimeoutError))
                            and runtime.unreported_fatal_settlement_error is None
                        ):
                            runtime.unreported_fatal_settlement_error = exc
                else:
                    callback_succeeded = True
        else:
            try:
                callback(runtime, error)
            except BaseException as exc:
                cleanup_errors.append(exc)
            else:
                callback_succeeded = True
        if callback_succeeded or settle is None:
            runtime.fatal_settled = True
    with runtime.control_lock:
        if cleanup_errors:
            runtime.cleanup_error = cleanup_errors[0]
        runtime.push_cleanup_error = push_cleanup_error
        runtime.deferred_cleanup_error = _first_non_push_cleanup_error(cleanup_errors, push_cleanup_error)
        runtime.started.set()
        runtime.stopped.set()


def _finish_runtime(runtime: ActorRuntime, initial_cleanup_error: BaseException | None = None) -> None:
    error = runtime.fatal_error or ConnectionClosedError("7709 Actor stopped")
    cleanup_errors: list[BaseException] = [] if initial_cleanup_error is None else [initial_cleanup_error]
    selector = runtime.selector
    reader = runtime.wake_reader
    writer = runtime.wake_writer
    selector_close_failed = selector is not None
    reader_close_failed = reader is not None
    writer_close_failed = writer is not None
    try:
        with runtime.control_lock:
            pending = runtime.pending_task
            if pending is not None:
                _claim_ticket_terminal(pending)
            runtime.pending_task = None
            active = runtime.active_task
            if active is not None:
                _claim_ticket_terminal(active)
            runtime.active_task = None
            runtime.cancel_requests.clear()
        for ticket in (pending, active):
            if ticket is None:
                continue
            try:
                _complete_ticket(ticket, RequestState.CANCELLED, error=error, terminal_claimed=True)
            except BaseException as exc:
                cleanup_errors.append(exc)
        try:
            _drop_generation(runtime, error)
        except BaseException as exc:
            cleanup_errors.append(exc)
        if runtime.push_buffer is not None and runtime.owns_push_buffer:
            runtime.push_buffer.publish_close(runtime.fatal_error)

        if selector is not None and reader is not None:
            try:
                selector.unregister(reader)
            except (KeyError, ValueError):
                pass
            except BaseException as exc:
                cleanup_errors.append(exc)
        for name, item in (("reader", reader), ("writer", writer)):
            if item is not None:
                try:
                    item.close()
                except BaseException as exc:
                    cleanup_errors.append(exc)
                else:
                    if name == "reader":
                        reader_close_failed = False
                    else:
                        writer_close_failed = False
        if selector is not None:
            try:
                selector.close()
            except BaseException as exc:
                cleanup_errors.append(exc)
            else:
                selector_close_failed = False
    except BaseException as exc:
        cleanup_errors.append(exc)
    finally:
        notify_cleanup_failure = False
        if cleanup_errors:
            with runtime.control_lock:
                cleanup_error = cleanup_errors[0]
                if runtime.fatal_error is None:
                    runtime.fatal_error = cleanup_error
                    runtime.last_error = cleanup_error
                    notify_cleanup_failure = True
                if runtime.cleanup_error is None:
                    runtime.cleanup_error = cleanup_error
                elif (
                    runtime.cleanup_error is runtime.push_cleanup_error
                    and runtime.deferred_cleanup_error is None
                ):
                    runtime.deferred_cleanup_error = cleanup_error
        if notify_cleanup_failure:
            _publish_fatal(runtime, cleanup_errors[0])
        with runtime.control_lock:
            runtime.selector = selector if selector_close_failed else None
            runtime.wake_reader = reader if reader_close_failed else None
            runtime.wake_writer = writer if writer_close_failed else None
            if runtime.state is RuntimeState.FAILED_CLOSING:
                if runtime.cleanup_error is None:
                    runtime.state = RuntimeState.FAILED_CLOSED
            elif runtime.fatal_error is not None:
                runtime.state = RuntimeState.FAILED
            else:
                runtime.state = RuntimeState.STOPPED
            runtime.stopped.set()
            runtime.started.set()


def _first_non_push_cleanup_error(
    cleanup_errors: list[BaseException],
    push_cleanup_error: BaseException | None,
) -> BaseException | None:
    skipped_push = False
    for error in cleanup_errors:
        if push_cleanup_error is not None and not skipped_push and error is push_cleanup_error:
            skipped_push = True
            continue
        return error
    return None


def _claim_ticket_terminal(ticket: ConnectTicket | RequestTicket) -> bool:
    with ticket.lock:
        if ticket.terminal_claimed or ticket.state in TERMINAL_REQUEST_STATES:
            return False
        ticket.terminal_claimed = True
        return True
