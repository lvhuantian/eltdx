"""Single-threaded non-blocking connection Actor for the 7709 transport."""

from __future__ import annotations

import errno
import selectors
import socket
import threading
import time
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
from eltdx.protocol.constants import TYPE_HANDSHAKE
from eltdx.protocol.frame import ResponseFrame, ResponseFrameDecoder


SelectorFactory = Callable[[], selectors.BaseSelector]
SocketFactory = Callable[[int, int, int], socket.socket]


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
    state: RequestState = RequestState.ADMITTED
    connected_host: str | None = None
    error: BaseException | None = None
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
    completion: Callable[[RequestTicket], None] | None = None
    attempts: int = 0
    state: RequestState = RequestState.ADMITTED
    result: object | None = None
    error: BaseException | None = None
    completed: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass(slots=True)
class TcpGeneration:
    generation_id: int
    sock: socket.socket
    endpoint: ResolvedEndpoint
    state: TcpState
    decoder: ResponseFrameDecoder = field(default_factory=ResponseFrameDecoder)
    tx_bytes: bytes = b""
    tx_offset: int = 0
    active_exchange: WireExchange | None = None
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
    sent_any: bool = False


@dataclass(frozen=True, slots=True)
class CancelToken:
    runtime_epoch: int
    tcp_generation: int
    lease_id: int


@dataclass(frozen=True, slots=True)
class FrameEnvelope:
    runtime_epoch: int
    tcp_generation: int
    lease_id: int
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


@dataclass(slots=True)
class ActorRuntime:
    runtime_epoch: int
    endpoints: tuple[ResolvedEndpoint, ...]
    selector_factory: SelectorFactory = selectors.DefaultSelector
    socket_factory: SocketFactory = socket.socket
    control_lock: threading.Lock = field(default_factory=threading.Lock)
    state: RuntimeState = RuntimeState.STARTING
    stop_requested: bool = False
    pending_task: ConnectTicket | RequestTicket | None = None
    cancel_request: CancelToken | None = None
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
    generation_counter: int = 0
    reconnect_count: int = 0
    connected_host: str | None = None
    last_error: BaseException | None = None
    msg_id_counter: int = 0
    stale_event_count: int = 0
    last_handshake: object | None = None


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
        getattr(socket, "EINPROGRESS", None),
        getattr(socket, "EWOULDBLOCK", None),
        getattr(socket, "EALREADY", None),
        getattr(socket, "EINTR", None),
    )
    if value is not None
)


def start_actor(
    runtime_epoch: int,
    endpoints: Sequence[ResolvedEndpoint],
    *,
    selector_factory: SelectorFactory = selectors.DefaultSelector,
    socket_factory: SocketFactory = socket.socket,
    startup_timeout: float = 1.0,
) -> ActorRuntime:
    runtime = ActorRuntime(
        runtime_epoch=runtime_epoch,
        endpoints=tuple(endpoints),
        selector_factory=selector_factory,
        socket_factory=socket_factory,
    )
    thread = threading.Thread(
        target=_run_actor,
        args=(runtime,),
        name=f"eltdx-7709-actor-{runtime_epoch}",
        daemon=True,
    )
    runtime.actor_thread = thread
    thread.start()
    if not runtime.started.wait(startup_timeout):
        request_actor_stop(runtime)
        raise TransportError("7709 Actor failed to start")
    if runtime.fatal_error is not None:
        raise TransportError("7709 Actor failed during startup") from runtime.fatal_error
    return runtime


def submit_connect(runtime: ActorRuntime, deadline: float) -> ConnectTicket:
    ticket = ConnectTicket(runtime_epoch=runtime.runtime_epoch, deadline=deadline)
    with runtime.control_lock:
        if runtime.state is not RuntimeState.RUNNING or runtime.stop_requested:
            raise ConnectionClosedError(f"7709 Actor is not running: {runtime.state.name}")
        if runtime.pending_task is not None:
            raise TransportError("7709 Actor mailbox is full")
        runtime.pending_task = ticket
    _notify_actor(runtime)
    return ticket


def submit_request(
    runtime: ActorRuntime,
    *,
    lease_id: int,
    command: int,
    payload: object,
    deadline: float,
    retry_safe: bool,
    completion: Callable[[RequestTicket], None] | None = None,
) -> RequestTicket:
    ticket = RequestTicket(
        runtime_epoch=runtime.runtime_epoch,
        lease_id=lease_id,
        command=command,
        request_payload_snapshot=payload,
        deadline=deadline,
        retry_safe=retry_safe,
        completion=completion,
    )
    with runtime.control_lock:
        if runtime.state is not RuntimeState.RUNNING or runtime.stop_requested:
            raise ConnectionClosedError(f"7709 Actor is not running: {runtime.state.name}")
        if runtime.pending_task is not None:
            raise TransportError("7709 Actor mailbox is full")
        runtime.pending_task = ticket
    _notify_actor(runtime)
    return ticket


def cancel_ticket(runtime: ActorRuntime, ticket: RequestTicket) -> None:
    with runtime.control_lock:
        generation = runtime.generation
        runtime.cancel_request = CancelToken(
            runtime_epoch=runtime.runtime_epoch,
            tcp_generation=generation.generation_id if generation is not None else 0,
            lease_id=ticket.lease_id,
        )
    _notify_actor(runtime)


def wait_ticket(ticket: ConnectTicket | RequestTicket) -> Any:
    remaining = max(0.0, ticket.deadline - time.monotonic())
    ticket.completed.wait(remaining + 0.05)
    if not ticket.completed.is_set():
        raise ResponseTimeoutError("7709 response timed out while awaiting Actor completion")
    if ticket.error is not None:
        raise ticket.error
    if isinstance(ticket, ConnectTicket):
        return ticket.connected_host
    return ticket.result


def request_actor_stop(runtime: ActorRuntime) -> None:
    with runtime.control_lock:
        runtime.stop_requested = True
        if runtime.state in (RuntimeState.STARTING, RuntimeState.RUNNING, RuntimeState.FAILED):
            runtime.state = RuntimeState.CLOSING
    _notify_actor(runtime)


def close_actor(runtime: ActorRuntime, timeout: float = 1.0) -> None:
    request_actor_stop(runtime)
    thread = runtime.actor_thread
    if thread is not None and thread is not threading.current_thread():
        thread.join(max(0.0, timeout))
    if thread is not None and thread.is_alive():
        with runtime.control_lock:
            runtime.state = RuntimeState.FAILED_CLOSING
        raise TransportCloseTimeoutError("7709 Actor did not stop within 1 second")
    with runtime.control_lock:
        if runtime.state is RuntimeState.FAILED_CLOSING:
            runtime.state = RuntimeState.FAILED_CLOSED


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
                runtime.state = RuntimeState.CLOSING
            else:
                runtime.state = RuntimeState.RUNNING
            runtime.started.set()

        while True:
            _drain_control(runtime)
            if runtime.stop_requested:
                break
            _expire_active_task(runtime)
            timeout = _selector_timeout(runtime)
            events = selector.select(timeout)
            for key, _ in events:
                token = key.data
                if isinstance(token, SelectorToken) and token.kind == "wakeup":
                    _drain_wakeup(runtime)
            _drain_control(runtime)
            if runtime.stop_requested:
                break
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
    finally:
        _finish_runtime(runtime)


def _drain_control(runtime: ActorRuntime) -> None:
    with runtime.control_lock:
        cancel = runtime.cancel_request
        runtime.cancel_request = None
    if cancel is not None:
        _apply_cancel(runtime, cancel)
    with runtime.control_lock:
        if runtime.stop_requested or runtime.active_task is not None:
            return
        task = runtime.pending_task
        runtime.pending_task = None
        runtime.active_task = task
    if task is None:
        return
    if isinstance(task, ConnectTicket):
        generation = runtime.generation
        if generation is not None and generation.state in (TcpState.CONNECTED_UNHANDSHAKEN, TcpState.READY):
            _complete_ticket(task, RequestState.SUCCESS, connected_host=generation.endpoint.host)
            runtime.active_task = None
            return
        runtime.endpoint_index = 0
        _start_next_endpoint(runtime)
        return
    if isinstance(task, RequestTicket):
        _start_request_attempt(runtime)


def _apply_cancel(runtime: ActorRuntime, cancel: CancelToken) -> None:
    if cancel.runtime_epoch != runtime.runtime_epoch:
        return
    ticket = runtime.active_task
    if isinstance(ticket, RequestTicket) and ticket.lease_id == cancel.lease_id:
        generation = runtime.generation
        if generation is not None and cancel.tcp_generation not in (0, generation.generation_id):
            runtime.stale_event_count += 1
            return
        exchange = generation.active_exchange if generation is not None else None
        if exchange is not None and exchange.sent_any:
            _drop_generation(runtime, ConnectionClosedError("7709 request cancelled after send"))
        _complete_ticket(ticket, RequestState.CANCELLED, error=ConnectionClosedError("7709 request cancelled"))
        runtime.active_task = None
        return
    with runtime.control_lock:
        pending = runtime.pending_task
        if isinstance(pending, RequestTicket) and pending.lease_id == cancel.lease_id:
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
    generation = runtime.generation
    if generation is None:
        runtime.endpoint_index = 0
        _start_next_endpoint(runtime)
        return
    if generation.state is TcpState.CONNECTED_UNHANDSHAKEN and ticket.command != TYPE_HANDSHAKE:
        _begin_exchange(runtime, ticket, TYPE_HANDSHAKE, handshake=True)
        return
    _begin_exchange(runtime, ticket, ticket.command, handshake=False)


def _begin_exchange(runtime: ActorRuntime, ticket: RequestTicket, command: int, *, handshake: bool) -> None:
    generation = runtime.generation
    if generation is None:
        raise RuntimeError("cannot start wire exchange without a TCP generation")
    runtime.msg_id_counter = 1 if runtime.msg_id_counter >= 0xFFFFFFFF else runtime.msg_id_counter + 1
    payload = {} if handshake else ticket.request_payload_snapshot
    if not isinstance(payload, dict):
        payload = dict(payload)  # type: ignore[arg-type]
    request = build_command_frame(command, payload, runtime.msg_id_counter)
    frame = request.to_bytes()
    generation.tx_bytes = frame
    generation.tx_offset = 0
    generation.active_exchange = WireExchange(
        ticket=ticket,
        command=command,
        msg_id=request.msg_id,
        msg_type=request.msg_type,
        frame=frame,
        handshake=handshake,
    )
    generation.state = TcpState.HANDSHAKING if handshake else TcpState.READY
    if not handshake:
        ticket.state = RequestState.SENDING
    _set_generation_interest(runtime, generation, selectors.EVENT_READ | selectors.EVENT_WRITE)


def _start_next_endpoint(runtime: ActorRuntime) -> None:
    ticket = runtime.active_task
    if not isinstance(ticket, (ConnectTicket, RequestTicket)):
        return
    while runtime.endpoint_index < len(runtime.endpoints):
        if time.monotonic() >= ticket.deadline:
            _fail_active_task(runtime, ResponseTimeoutError("7709 response timed out during connect"), retryable=False)
            return
        endpoint = runtime.endpoints[runtime.endpoint_index]
        runtime.endpoint_index += 1
        runtime.generation_counter += 1
        sock = runtime.socket_factory(endpoint.family, endpoint.socktype, endpoint.proto)
        sock.setblocking(False)
        generation = TcpGeneration(
            generation_id=runtime.generation_counter,
            sock=sock,
            endpoint=endpoint,
            state=TcpState.CONNECTING,
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
            if runtime.generation is generation and mask & selectors.EVENT_READ:
                _receive_generation(runtime, generation)
        except ProtocolError as exc:
            _handle_wire_failure(runtime, exc, retryable=False)
        except (OSError, ConnectionClosedError) as exc:
            _handle_wire_failure(runtime, exc, retryable=True)
        return
    error_code = generation.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if error_code == 0:
        _finish_connect(runtime, generation)
        return
    if error_code in _IN_PROGRESS_CONNECT_CODES:
        return
    error = OSError(error_code, f"connect failed for {generation.endpoint.host}")
    runtime.last_error = error
    _drop_generation(runtime, error)
    _start_next_endpoint(runtime)


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
    generation.tx_offset += sent
    generation.last_activity_at = time.monotonic()
    if generation.tx_offset < len(generation.tx_bytes):
        return
    if not exchange.handshake:
        exchange.ticket.state = RequestState.WAITING_RESPONSE
    _set_generation_interest(runtime, generation, selectors.EVENT_READ)


def _receive_generation(runtime: ActorRuntime, generation: TcpGeneration) -> None:
    bytes_read = 0
    frames_read = 0
    while bytes_read < 256 * 1024 and frames_read < 64:
        try:
            chunk = generation.sock.recv(min(64 * 1024, 256 * 1024 - bytes_read))
        except BlockingIOError:
            return
        if not chunk:
            generation.decoder.finish()
            raise ConnectionClosedError("7709 socket closed by remote peer")
        bytes_read += len(chunk)
        generation.last_activity_at = time.monotonic()
        frames = generation.decoder.feed(chunk)
        for response in frames:
            frames_read += 1
            _route_frame(runtime, generation, response)
            if runtime.generation is not generation or frames_read >= 64:
                return


def _route_frame(runtime: ActorRuntime, generation: TcpGeneration, response: ResponseFrame) -> None:
    exchange = generation.active_exchange
    active = runtime.active_task
    if (
        exchange is None
        or exchange.ticket is not active
        or exchange.ticket.runtime_epoch != runtime.runtime_epoch
        or response.msg_id != exchange.msg_id
        or response.msg_type != exchange.msg_type
    ):
        runtime.stale_event_count += 1
        return

    ticket = exchange.ticket
    if exchange.handshake:
        runtime.last_handshake = parse_command_response(TYPE_HANDSHAKE, response, {})
        generation.active_exchange = None
        generation.tx_bytes = b""
        generation.tx_offset = 0
        generation.state = TcpState.READY
        if ticket.command != TYPE_HANDSHAKE:
            _begin_exchange(runtime, ticket, ticket.command, handshake=False)
            return
    elif ticket.command == TYPE_HANDSHAKE:
        runtime.last_handshake = parse_command_response(TYPE_HANDSHAKE, response, {})
    envelope = FrameEnvelope(
        runtime_epoch=runtime.runtime_epoch,
        tcp_generation=generation.generation_id,
        lease_id=ticket.lease_id,
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
        if ticket.command == TYPE_HANDSHAKE:
            _begin_exchange(runtime, ticket, TYPE_HANDSHAKE, handshake=False)
        else:
            _begin_exchange(runtime, ticket, TYPE_HANDSHAKE, handshake=True)


def _expire_active_task(runtime: ActorRuntime) -> None:
    ticket = runtime.active_task
    if ticket is None or time.monotonic() < ticket.deadline:
        return
    generation = runtime.generation
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
    _drop_generation(runtime, error)
    _fail_active_task(runtime, error, retryable=True, sent_business=sent_business)


def _selector_timeout(runtime: ActorRuntime) -> float | None:
    ticket = runtime.active_task
    if ticket is None:
        return None
    return max(0.0, ticket.deadline - time.monotonic())


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
            cancel = runtime.cancel_request
            if cancel is not None and cancel.runtime_epoch == runtime.runtime_epoch and cancel.lease_id == ticket.lease_id:
                runtime.cancel_request = None
            else:
                cancel = None
        if cancel is not None:
            _complete_ticket(ticket, RequestState.CANCELLED, error=ConnectionClosedError("7709 request cancelled"))
            runtime.active_task = None
            return
        can_retry = (
            retryable
            and ticket.attempts < 2
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
        if isinstance(ticket, ConnectTicket):
            ticket.connected_host = connected_host
        else:
            ticket.result = result
    try:
        if isinstance(ticket, RequestTicket) and ticket.completion is not None:
            ticket.completion(ticket)
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


def _notify_actor(runtime: ActorRuntime) -> None:
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


def _finish_runtime(runtime: ActorRuntime) -> None:
    error = runtime.fatal_error or ConnectionClosedError("7709 Actor stopped")
    with runtime.control_lock:
        pending = runtime.pending_task
        runtime.pending_task = None
        active = runtime.active_task
        runtime.active_task = None
    for ticket in (pending, active):
        if ticket is not None:
            _complete_ticket(ticket, RequestState.CANCELLED, error=error)
    _drop_generation(runtime, error)

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
