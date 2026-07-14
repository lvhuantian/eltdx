"""Single-threaded non-blocking connection Actor for the 7709 transport."""

from __future__ import annotations

import errno
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
from eltdx.protocol.frame import ResponseFrame, ResponseFrameDecoder

from .push import PushBuffer, PushFrame


SelectorFactory = Callable[[], selectors.BaseSelector]
SocketFactory = Callable[[int, int, int], socket.socket]
CandidateCallback = Callable[["ActorRuntime"], None]


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
    receive_drained: bool = False
    connected_at: float = 0.0
    last_activity_at: float = 0.0


@dataclass(slots=True)
class WireExchange:
    ticket: RequestTicket
    command: int
    msg_id: int
    msg_type: int
    frame: bytes
    handshake: bool
    rx_boundary: int = 0
    sent_any: bool = False


@dataclass(frozen=True, slots=True)
class CancelToken:
    runtime_epoch: int
    request_id: int
    lease_id: int


@dataclass(frozen=True, slots=True)
class ReceivedFrame:
    sequence: int
    response: ResponseFrame


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
    fatal_callback: Callable[[ActorRuntime, BaseException], None] | None = None
    control_lock: threading.Lock = field(default_factory=threading.Lock)
    state: RuntimeState = RuntimeState.STARTING
    stop_requested: bool = False
    pending_task: ConnectTicket | RequestTicket | None = None
    cancel_requests: dict[int, CancelToken] = field(default_factory=dict)
    started: threading.Event = field(default_factory=threading.Event)
    stopped: threading.Event = field(default_factory=threading.Event)
    generation_started: threading.Event = field(default_factory=threading.Event)
    fatal_error: BaseException | None = None
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
    request_id_counter: int = 0
    stale_event_count: int = 0
    last_handshake: object | None = None
    last_heartbeat: object | None = None


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
    fatal_callback: Callable[[ActorRuntime, BaseException], None] | None = None,
    candidate_callback: CandidateCallback | None = None,
    startup_timeout: float = 1.0,
) -> ActorRuntime:
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
        fatal_callback=fatal_callback,
    )
    thread = threading.Thread(
        target=_run_actor,
        args=(runtime,),
        name=f"eltdx-7709-actor-{runtime_epoch}",
        daemon=True,
    )
    runtime.actor_thread = thread
    try:
        if candidate_callback is not None:
            candidate_callback(runtime)
        thread.start()
    except BaseException as exc:
        _fail_actor_startup(runtime, exc)
        raise ActorStartupError("7709 Actor failed before thread startup", runtime) from exc
    if not runtime.started.wait(startup_timeout):
        request_actor_stop(runtime)
        raise ActorStartupError("7709 Actor failed to start", runtime)
    if runtime.fatal_error is not None:
        raise ActorStartupError("7709 Actor failed during startup", runtime) from runtime.fatal_error
    return runtime


def submit_connect(
    runtime: ActorRuntime,
    deadline: float,
    lease_id: int = 0,
    completion: Callable[[ConnectTicket | None], None] | None = None,
) -> ConnectTicket:
    with runtime.control_lock:
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
        runtime.pending_task = ticket
        writer = runtime.wake_writer
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
) -> RequestTicket:
    with runtime.control_lock:
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
        runtime.pending_task = ticket
        writer = runtime.wake_writer
    _notify_submitted_ticket(runtime, ticket, writer)
    return ticket


def cancel_ticket(runtime: ActorRuntime, ticket: ConnectTicket | RequestTicket) -> None:
    with runtime.control_lock:
        if ticket.runtime_epoch != runtime.runtime_epoch or (
            ticket is not runtime.active_task and ticket is not runtime.pending_task
        ):
            return
        runtime.cancel_requests[ticket.request_id] = CancelToken(
            runtime_epoch=runtime.runtime_epoch,
            request_id=ticket.request_id,
            lease_id=ticket.lease_id,
        )
        writer = runtime.wake_writer
    _notify_actor(runtime, writer)


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
    if ticket.state is RequestState.SUCCESS and ticket.completed_at is not None and ticket.completed_at > ticket.deadline:
        raise ResponseTimeoutError("7709 response timed out during Actor completion")
    if ticket.error is not None:
        raise ticket.error
    if isinstance(ticket, ConnectTicket):
        return ticket.connected_host
    return ticket.result


def request_actor_stop(runtime: ActorRuntime) -> None:
    with runtime.control_lock:
        runtime.stop_requested = True
        thread = runtime.actor_thread
        if runtime.state in (RuntimeState.STARTING, RuntimeState.RUNNING) or (
            runtime.state is RuntimeState.FAILED and thread is not None and thread.is_alive()
        ):
            runtime.state = RuntimeState.CLOSING
        writer = runtime.wake_writer
    _notify_actor(runtime, writer)


def close_actor(runtime: ActorRuntime, timeout: float = 1.0) -> None:
    with runtime.control_lock:
        failed_before_close = runtime.fatal_error is not None or runtime.state in (
            RuntimeState.FAILED,
            RuntimeState.FAILED_CLOSING,
            RuntimeState.FAILED_CLOSED,
        )
    request_actor_stop(runtime)
    thread = runtime.actor_thread
    if thread is not None and thread.ident is not None and thread is not threading.current_thread():
        thread.join(max(0.0, timeout))
    if thread is not None and thread.is_alive():
        with runtime.control_lock:
            runtime.state = RuntimeState.FAILED_CLOSING
        raise TransportCloseTimeoutError("7709 Actor did not stop within 1 second")
    with runtime.control_lock:
        if failed_before_close or runtime.state is RuntimeState.FAILED_CLOSING:
            runtime.state = RuntimeState.FAILED_CLOSED


def abandon_actor(runtime: ActorRuntime) -> None:
    """Best-effort non-blocking finalizer callback for one exact runtime."""

    request_actor_stop(runtime)


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
        wake_reader, wake_writer = socket.socketpair()
        wake_reader.setblocking(False)
        wake_writer.setblocking(False)
        selector.register(
            wake_reader,
            selectors.EVENT_READ,
            SelectorToken("wakeup", runtime.runtime_epoch, 0, wake_reader),
        )
        with runtime.control_lock:
            runtime.selector = selector
            runtime.wake_reader = wake_reader
            runtime.wake_writer = wake_writer
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
            for key, _ in events:
                token = key.data
                if isinstance(token, SelectorToken) and token.kind == "wakeup":
                    _drain_wakeup(runtime)
            _drain_control(runtime)
            if runtime.stop_requested:
                break
            _expire_active_task(runtime)
            for key, mask in events:
                token = key.data
                if isinstance(token, SelectorToken) and token.kind == "tcp":
                    _handle_tcp_event(runtime, token, mask)
            _expire_active_task(runtime)
    except BaseException as exc:
        with runtime.control_lock:
            runtime.fatal_error = exc
            runtime.last_error = exc
            runtime.state = RuntimeState.FAILED
        if runtime.fatal_callback is not None:
            runtime.fatal_callback(runtime, exc)
    finally:
        _finish_runtime(runtime)


def _drain_control(runtime: ActorRuntime) -> None:
    with runtime.control_lock:
        cancels = tuple(runtime.cancel_requests.values())
        runtime.cancel_requests.clear()
    for cancel in cancels:
        _apply_cancel(runtime, cancel)
    with runtime.control_lock:
        if runtime.stop_requested or runtime.active_task is not None:
            return
        task = runtime.pending_task
        runtime.pending_task = None
        runtime.active_task = task
    generation = runtime.generation
    if isinstance(task, RequestTicket) and generation is not None and generation.active_exchange is None:
        generation.receive_drained = False


def _advance_active_task(runtime: ActorRuntime) -> None:
    task = runtime.active_task
    if isinstance(task, ConnectTicket):
        generation = runtime.generation
        if generation is not None and generation.state in (TcpState.CONNECTED_UNHANDSHAKEN, TcpState.READY):
            _complete_ticket(task, RequestState.SUCCESS, connected_host=generation.endpoint.host)
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
    if generation.active_exchange is not None or generation.state is TcpState.CONNECTING:
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
            if exchange is not None and exchange.ticket is ticket:
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
            runtime.pending_task = None
        else:
            pending = None
    if pending is not None:
        _complete_ticket(pending, RequestState.CANCELLED, error=ConnectionClosedError("7709 request cancelled"))


def _start_request_attempt(runtime: ActorRuntime) -> None:
    ticket = runtime.active_task
    if not isinstance(ticket, RequestTicket):
        return
    if time.monotonic() >= ticket.deadline:
        _complete_ticket(ticket, RequestState.FAILED, error=ResponseTimeoutError("7709 response timed out before connect"))
        runtime.active_task = None
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
    next_msg_id = 1 if runtime.msg_id_counter >= 0xFFFFFFFF else runtime.msg_id_counter + 1
    payload = {} if handshake else ticket.request_payload_snapshot
    try:
        if not isinstance(payload, dict):
            payload = dict(payload)  # type: ignore[arg-type]
        request = build_command_frame(command, payload, next_msg_id)
        frame = request.to_bytes()
    except Exception as exc:
        if runtime.active_task is ticket:
            runtime.active_task = None
        _complete_ticket(ticket, RequestState.FAILED, error=exc)
        return False
    runtime.msg_id_counter = next_msg_id
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
    )
    generation.state = TcpState.HANDSHAKING if handshake else TcpState.READY
    if not handshake:
        ticket.state = RequestState.SENDING
    _set_generation_interest(runtime, generation, selectors.EVENT_READ | selectors.EVENT_WRITE)
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
        sock: socket.socket | None = None
        try:
            sock = runtime.socket_factory(endpoint.family, endpoint.socktype, endpoint.proto)
            sock.setblocking(False)
        except OSError as exc:
            if sock is not None:
                sock.close()
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


def _set_generation_interest(runtime: ActorRuntime, generation: TcpGeneration, events: int) -> None:
    selector = runtime.selector
    if selector is None:
        raise RuntimeError("Actor selector is unavailable")
    token = SelectorToken("tcp", runtime.runtime_epoch, generation.generation_id, generation.sock)
    try:
        selector.modify(generation.sock, events, token)
    except KeyError:
        selector.register(generation.sock, events, token)


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
        try:
            if mask & selectors.EVENT_WRITE:
                _send_generation(runtime, generation)
        except (OSError, ConnectionClosedError) as exc:
            _handle_wire_failure(runtime, exc, retryable=True)
            return
        if runtime.generation is generation and mask & selectors.EVENT_READ:
            generation.receive_drained = False
            _receive_generation_safely(runtime, generation)
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
    generation.connect_recheck_at = min(
        generation.connect_deadline,
        time.monotonic() + _CONNECT_RECHECK_INTERVAL,
    )


def _send_generation(runtime: ActorRuntime, generation: TcpGeneration) -> None:
    exchange = generation.active_exchange
    if exchange is None or generation.tx_offset >= len(generation.tx_bytes):
        return
    try:
        sent = generation.sock.send(memoryview(generation.tx_bytes)[generation.tx_offset:])
    except BlockingIOError:
        return
    if sent == 0:
        raise ConnectionClosedError("7709 socket closed during send")
    exchange.sent_any = True
    if not exchange.handshake and not exchange.ticket.retry_safe:
        exchange.ticket.attempt_deadline = exchange.ticket.deadline
    generation.tx_offset += sent
    generation.last_activity_at = time.monotonic()
    if generation.tx_offset < len(generation.tx_bytes):
        return
    if not exchange.handshake:
        exchange.ticket.state = RequestState.WAITING_RESPONSE
    _set_generation_interest(runtime, generation, selectors.EVENT_READ)


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
            chunk = generation.sock.recv(min(64 * 1024, 256 * 1024 - bytes_read))
        except BlockingIOError:
            generation.receive_drained = True
            return True
        if not chunk:
            generation.decoder.finish()
            raise ConnectionClosedError("7709 socket closed by remote peer")
        generation.receive_drained = False
        bytes_read += len(chunk)
        generation.last_activity_at = time.monotonic()
        frames = generation.decoder.feed(chunk)
        if len(generation.decoded_frames) + len(frames) > 1024:
            raise ProtocolError("decoded response frame queue exceeds limit: 1024")
        for response in frames:
            generation.rx_sequence += 1
            generation.decoded_frames.append(ReceivedFrame(generation.rx_sequence, response))
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
                PushFrame(runtime.runtime_epoch, generation.generation_id, generation.endpoint.host, response)
            )
        return

    ticket = exchange.ticket
    if exchange.handshake:
        runtime.last_handshake = parse_command_response(TYPE_HANDSHAKE, response, {})
        generation.active_exchange = None
        generation.tx_bytes = b""
        generation.tx_offset = 0
        generation.state = TcpState.READY
        if ticket.command != TYPE_HANDSHAKE:
            ticket.state = RequestState.ADMITTED
            _set_generation_interest(runtime, generation, selectors.EVENT_READ)
            return
    elif ticket.command == TYPE_HANDSHAKE:
        runtime.last_handshake = parse_command_response(TYPE_HANDSHAKE, response, {})
    elif ticket.command == TYPE_HEARTBEAT:
        runtime.last_heartbeat = parse_command_response(TYPE_HEARTBEAT, response, {})
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
    runtime.active_task = None
    _set_generation_interest(runtime, generation, selectors.EVENT_READ)
    _complete_ticket(ticket, RequestState.SUCCESS, result=envelope)


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
    now = time.monotonic()
    generation.state = TcpState.CONNECTED_UNHANDSHAKEN
    generation.connected_at = now
    generation.last_activity_at = now
    runtime.connected_host = generation.endpoint.host
    ticket = runtime.active_task
    if isinstance(ticket, ConnectTicket):
        _complete_ticket(ticket, RequestState.SUCCESS, connected_host=generation.endpoint.host)
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
    if interval is None or interval <= 0 or generation is None or runtime.active_task is not None:
        return
    with runtime.control_lock:
        if runtime.pending_task is not None or runtime.cancel_requests:
            return
    now = time.monotonic()
    if now < generation.last_activity_at + interval:
        return
    if runtime.heartbeat_allowed is not None and not runtime.heartbeat_allowed():
        generation.last_activity_at = now
        return
    with runtime.control_lock:
        runtime.request_id_counter += 1
        request_id = runtime.request_id_counter
    ticket = RequestTicket(
        runtime_epoch=runtime.runtime_epoch,
        lease_id=-1,
        command=TYPE_HEARTBEAT,
        request_payload_snapshot={},
        deadline=time.monotonic() + runtime.request_timeout,
        retry_safe=False,
        request_id=request_id,
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
    if isinstance(ticket, ConnectTicket):
        _complete_ticket(ticket, RequestState.FAILED, error=error)
        runtime.active_task = None
        return
    if isinstance(ticket, RequestTicket):
        with runtime.control_lock:
            cancel = runtime.cancel_requests.pop(ticket.request_id, None)
        if cancel is not None:
            _complete_ticket(ticket, RequestState.CANCELLED, error=ConnectionClosedError("7709 request cancelled"))
            runtime.active_task = None
            return
        can_retry = (
            retryable
            and not ticket.internal
            and ticket.attempts < _MAX_REQUEST_ATTEMPTS
            and time.monotonic() < ticket.deadline
            and (ticket.retry_safe or not sent_business)
        )
        if can_retry:
            ticket.state = RequestState.ADMITTED
            _start_request_attempt(runtime)
            return
        _complete_ticket(ticket, RequestState.FAILED, error=error)
        runtime.active_task = None


def _complete_ticket(
    ticket: ConnectTicket | RequestTicket,
    state: RequestState,
    *,
    connected_host: str | None = None,
    result: object | None = None,
    error: BaseException | None = None,
) -> bool:
    with ticket.lock:
        if ticket.state in TERMINAL_REQUEST_STATES:
            return False
        ticket.state = state
        ticket.error = error
        ticket.completed_at = time.monotonic()
        if isinstance(ticket, ConnectTicket):
            ticket.connected_host = connected_host
        else:
            ticket.result = result
    try:
        if ticket.completion is not None:
            try:
                ticket.completion(ticket)
            except Exception as exc:
                with ticket.lock:
                    ticket.completion_error = exc
    finally:
        ticket.completed.set()
    return True


def _drop_generation(runtime: ActorRuntime, reason: BaseException | None) -> None:
    generation = runtime.generation
    if generation is None:
        return
    generation.state = TcpState.RETIRING
    selector = runtime.selector
    if selector is not None:
        try:
            selector.unregister(generation.sock)
        except (KeyError, ValueError):
            pass
    try:
        generation.sock.close()
    finally:
        runtime.generation = None
        runtime.connected_host = None
        runtime.reconnect_count += 1
        if reason is not None:
            runtime.last_error = reason


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
                runtime.pending_task = None
        if withdrawn:
            _complete_ticket(ticket, RequestState.FAILED, error=exc)


def _fail_actor_startup(runtime: ActorRuntime, error: BaseException) -> None:
    with runtime.control_lock:
        runtime.fatal_error = error
        runtime.last_error = error
        runtime.state = RuntimeState.FAILED
        runtime.started.set()
        runtime.stopped.set()
    if runtime.push_buffer is not None and runtime.owns_push_buffer:
        runtime.push_buffer.close(error)
    if runtime.fatal_callback is not None:
        runtime.fatal_callback(runtime, error)


def _finish_runtime(runtime: ActorRuntime) -> None:
    error = runtime.fatal_error or ConnectionClosedError("7709 Actor stopped")
    with runtime.control_lock:
        pending = runtime.pending_task
        runtime.pending_task = None
        active = runtime.active_task
        runtime.active_task = None
        runtime.cancel_requests.clear()
    for ticket in (pending, active):
        if ticket is not None:
            _complete_ticket(ticket, RequestState.CANCELLED, error=error)
    _drop_generation(runtime, error)
    if runtime.push_buffer is not None and runtime.owns_push_buffer:
        runtime.push_buffer.close(runtime.fatal_error)

    selector = runtime.selector
    reader = runtime.wake_reader
    writer = runtime.wake_writer
    if selector is not None and reader is not None:
        try:
            selector.unregister(reader)
        except (KeyError, ValueError):
            pass
    for item in (reader, writer):
        if item is not None:
            try:
                item.close()
            except OSError:
                pass
    if selector is not None:
        selector.close()
    with runtime.control_lock:
        runtime.selector = None
        runtime.wake_reader = None
        runtime.wake_writer = None
        if runtime.state is RuntimeState.FAILED_CLOSING:
            runtime.state = RuntimeState.FAILED_CLOSED
        elif runtime.fatal_error is not None:
            runtime.state = RuntimeState.FAILED
        else:
            runtime.state = RuntimeState.STOPPED
        runtime.stopped.set()
        runtime.started.set()
