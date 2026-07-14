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
    ResponseTimeoutError,
    TransportCloseTimeoutError,
    TransportError,
)
from eltdx.hosts import ResolvedEndpoint
from eltdx.protocol.frame import ResponseFrameDecoder


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
    active_exchange: object | None = None
    connected_at: float = 0.0
    last_activity_at: float = 0.0


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
    cancel_request: object | None = None
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
        if runtime.stop_requested or runtime.active_task is not None:
            return
        task = runtime.pending_task
        runtime.pending_task = None
        runtime.active_task = task
    if task is None:
        return
    if isinstance(task, ConnectTicket):
        generation = runtime.generation
        if generation is not None and generation.state is TcpState.CONNECTED_UNHANDSHAKEN:
            _complete_ticket(task, RequestState.SUCCESS, connected_host=generation.endpoint.host)
            runtime.active_task = None
            return
        runtime.endpoint_index = 0
        _start_next_endpoint(runtime)
        return
    _complete_ticket(task, RequestState.FAILED, error=TransportError("request wire lifecycle is not active yet"))
    runtime.active_task = None


def _start_next_endpoint(runtime: ActorRuntime) -> None:
    ticket = runtime.active_task
    if not isinstance(ticket, ConnectTicket):
        return
    while runtime.endpoint_index < len(runtime.endpoints):
        if time.monotonic() >= ticket.deadline:
            _fail_connect_task(runtime, ResponseTimeoutError("7709 response timed out during connect"))
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
    _fail_connect_task(runtime, error)


def _register_connecting(runtime: ActorRuntime, generation: TcpGeneration) -> None:
    selector = runtime.selector
    if selector is None:
        raise RuntimeError("Actor selector is unavailable")
    selector.register(
        generation.sock,
        selectors.EVENT_READ | selectors.EVENT_WRITE,
        SelectorToken("tcp", runtime.runtime_epoch, generation.generation_id, generation.sock),
    )


def _handle_tcp_event(runtime: ActorRuntime, token: SelectorToken, mask: int) -> None:
    generation = runtime.generation
    if (
        token.runtime_epoch != runtime.runtime_epoch
        or generation is None
        or token.tcp_generation != generation.generation_id
        or token.sock is not generation.sock
    ):
        return
    if generation.state is not TcpState.CONNECTING:
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


def _expire_active_task(runtime: ActorRuntime) -> None:
    ticket = runtime.active_task
    if ticket is None or time.monotonic() < ticket.deadline:
        return
    _drop_generation(runtime, ResponseTimeoutError("7709 response timed out during connect"))
    _fail_connect_task(runtime, ResponseTimeoutError("7709 response timed out during connect"))


def _selector_timeout(runtime: ActorRuntime) -> float | None:
    ticket = runtime.active_task
    if ticket is None:
        return None
    return max(0.0, ticket.deadline - time.monotonic())


def _fail_connect_task(runtime: ActorRuntime, error: BaseException) -> None:
    ticket = runtime.active_task
    if isinstance(ticket, ConnectTicket):
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
