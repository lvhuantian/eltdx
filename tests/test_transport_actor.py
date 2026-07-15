from __future__ import annotations

import errno
import selectors
import socket
import threading
import time

import pytest

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx.exceptions import ConnectionClosedError, ResponseTimeoutError, TransportCloseTimeoutError
from eltdx.hosts import normalize_host, probe_host, resolve_host, resolve_hosts
from eltdx.protocol.commands import parse_command_response
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_SECURITY_COUNT
from eltdx.transport.actor import (
    ActorStartupError,
    FrameEnvelope,
    RuntimeState,
    TcpState,
    actor_snapshot,
    close_actor,
    cancel_ticket,
    request_actor_stop,
    start_actor,
    submit_connect,
    submit_request,
    wait_ticket,
)
from eltdx.transport import actor as actor_module


def test_actor_connects_real_loopback_and_close_releases_every_resource() -> None:
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        if not release.wait(timeout=2):
            raise AssertionError("connection release was not signaled")

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(1, resolve_hosts([server.host]))
        try:
            ticket = submit_connect(runtime, time.monotonic() + 1)
            assert wait_ticket(ticket) == server.host
            snapshot = actor_snapshot(runtime)
            assert snapshot.state is RuntimeState.RUNNING
            assert snapshot.tcp_state is TcpState.CONNECTED_UNHANDSHAKEN
            assert snapshot.actor_alive
        finally:
            release.set()
            close_actor(runtime)

    assert runtime.state is RuntimeState.STOPPED
    assert runtime.generation is None
    assert runtime.selector is None
    assert runtime.wake_reader is None
    assert runtime.wake_writer is None
    assert runtime.actor_thread is not None and not runtime.actor_thread.is_alive()


def test_numeric_endpoint_resolution_never_calls_blocking_dns(monkeypatch) -> None:
    resolve_host.cache_clear()
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("DNS called")))

    endpoint = resolve_host("127.0.0.1:7709")[0]

    assert endpoint.sockaddr == ("127.0.0.1", 7709)


@pytest.mark.parametrize(
    ("host", "normalized"),
    [
        ("127.0.0.1:0", None),
        ("127.0.0.1:1", "127.0.0.1:1"),
        ("127.0.0.1:65535", "127.0.0.1:65535"),
        ("127.0.0.1:65536", None),
        ("127.0.0.1:99999", None),
        ("[::1]:00080", "[::1]:80"),
    ],
)
def test_host_port_normalization_boundaries(host: str, normalized: str | None) -> None:
    assert normalize_host(host) == normalized


def test_probe_bracketed_ipv6_uses_unbracketed_socket_address(monkeypatch) -> None:
    captured: list[tuple[tuple[str, int], float]] = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback) -> None:
            return None

    def create_connection(address, timeout):
        captured.append((address, timeout))
        return Connection()

    monkeypatch.setattr(socket, "create_connection", create_connection)

    result = probe_host("[::1]:7709", timeout=0.25)

    assert result.ok and result.host == "[::1]:7709"
    assert captured == [(('::1', 7709), 0.25)]


def test_invalid_port_never_reaches_dns_or_probe_socket(monkeypatch) -> None:
    resolve_host.cache_clear()
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("DNS called")))
    monkeypatch.setattr(
        socket,
        "create_connection",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("socket probe called")),
    )

    with pytest.raises(ValueError, match="invalid host"):
        resolve_host("127.0.0.1:65536")
    endpoints = resolve_hosts(["127.0.0.1:65536", "127.0.0.1:9"])
    result = probe_host("127.0.0.1:65536")

    assert [endpoint.host for endpoint in endpoints] == ["127.0.0.1:9"]
    assert not result.ok and result.error == "invalid host"


def test_actor_startup_register_failure_closes_selector_and_wakeup_pair(monkeypatch) -> None:
    selectors_created: list[FailingRegisterSelector] = []
    sockets_created: list[socket.socket] = []
    real_socketpair = socket.socketpair

    class FailingRegisterSelector(selectors.SelectSelector):
        def __init__(self) -> None:
            super().__init__()
            self.close_calls = 0
            selectors_created.append(self)

        def register(self, *args, **kwargs):
            raise RuntimeError("deterministic selector registration failure")

        def close(self) -> None:
            self.close_calls += 1
            super().close()

    def observed_socketpair():
        pair = real_socketpair()
        sockets_created.extend(pair)
        return pair

    monkeypatch.setattr(actor_module.socket, "socketpair", observed_socketpair)

    with pytest.raises(ActorStartupError, match="failed during startup") as raised:
        start_actor(
            18,
            resolve_hosts(["127.0.0.1:9"]),
            selector_factory=FailingRegisterSelector,
        )

    runtime = raised.value.runtime
    assert runtime.actor_thread is not None
    runtime.actor_thread.join(timeout=2)
    assert len(selectors_created) == 1 and selectors_created[0].close_calls == 1
    assert not selectors_created[0].get_map()
    assert len(sockets_created) == 2 and all(item.fileno() == -1 for item in sockets_created)
    assert runtime.selector is None
    assert runtime.wake_reader is None and runtime.wake_writer is None
    assert runtime.stopped.is_set() and runtime.state is RuntimeState.FAILED
    assert isinstance(runtime.fatal_error, RuntimeError)
    assert str(runtime.fatal_error) == "deterministic selector registration failure"
    assert not runtime.actor_thread.is_alive()


def test_pre_generation_socket_close_failure_is_retained_and_surfaced() -> None:
    created: list[FailingCandidateSocket] = []

    class FailingCandidateSocket:
        def __init__(self, *_args) -> None:
            self.allow_close = False
            self.closed = False
            self.close_calls = 0
            created.append(self)

        def setblocking(self, _value: bool) -> None:
            raise OSError("deterministic setblocking failure")

        def close(self) -> None:
            self.close_calls += 1
            if not self.allow_close:
                raise RuntimeError("deterministic candidate close failure")
            self.closed = True

        def fileno(self) -> int:
            return -1 if self.closed else 73

    runtime = start_actor(
        19,
        resolve_hosts(["127.0.0.1:9"]),
        socket_factory=FailingCandidateSocket,
    )
    ticket = submit_connect(runtime, time.monotonic() + 1)
    assert runtime.stopped.wait(timeout=2)
    with pytest.raises(RuntimeError, match="candidate close failure"):
        wait_ticket(ticket)

    assert len(created) == 1
    candidate = created[0]
    assert runtime.generation is not None and runtime.generation.sock is candidate
    assert isinstance(runtime.cleanup_error, RuntimeError)
    assert candidate.close_calls >= 2 and candidate.fileno() == 73
    with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
        close_actor(runtime)
    assert runtime.generation is not None and runtime.generation.sock is candidate

    candidate.allow_close = True
    candidate.close()
    with runtime.control_lock:
        runtime.generation = None
        runtime.cleanup_error = None
    close_actor(runtime)
    assert runtime.state is RuntimeState.FAILED_CLOSED


def test_actor_uses_so_error_and_fails_over_to_next_host() -> None:
    release = threading.Event()
    created = 0

    def factory(family: int, socktype: int, proto: int):
        nonlocal created
        created += 1
        if created == 1:
            return FailedSoErrorSocket(family, socktype, proto)
        return socket.socket(family, socktype, proto)

    def handler(conn: socket.socket) -> None:
        if not release.wait(timeout=2):
            raise AssertionError("connection release was not signaled")

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(2, resolve_hosts(["127.0.0.1:9", server.host]), socket_factory=factory)
        try:
            assert wait_ticket(submit_connect(runtime, time.monotonic() + 2)) == server.host
            assert runtime.generation_counter == 2
            assert runtime.reconnect_count == 1
        finally:
            release.set()
            close_actor(runtime)


class StalledSocket:
    def __init__(self, family: int, socktype: int, proto: int) -> None:
        self._socket, self._peer = socket.socketpair()
        self.closed_by: str | None = None
        self.so_error_calls = 0

    def setblocking(self, value: bool) -> None:
        self._socket.setblocking(value)

    def connect_ex(self, address) -> int:
        return errno.EINPROGRESS

    def getsockopt(self, level: int, option: int) -> int:
        self.so_error_calls += 1
        return errno.EINPROGRESS

    def fileno(self) -> int:
        return self._socket.fileno()

    def close(self) -> None:
        self.closed_by = threading.current_thread().name
        self._socket.close()
        self._peer.close()


class FailedSoErrorSocket(StalledSocket):
    def getsockopt(self, level: int, option: int) -> int:
        return errno.ECONNREFUSED


class ImmediateSocket(StalledSocket):
    def connect_ex(self, address) -> int:
        return 0


class ImmediateRefusalSocket(StalledSocket):
    def connect_ex(self, address) -> int:
        return errno.ECONNREFUSED


def test_connect_ex_immediate_success_and_immediate_refusal() -> None:
    runtime = start_actor(6, resolve_hosts(["127.0.0.1:9"]), socket_factory=ImmediateSocket)
    try:
        assert wait_ticket(submit_connect(runtime, time.monotonic() + 1)) == "127.0.0.1:9"
    finally:
        close_actor(runtime)

    runtime = start_actor(7, resolve_hosts(["127.0.0.1:9"]), socket_factory=ImmediateRefusalSocket)
    try:
        with pytest.raises(ConnectionClosedError, match="unable to connect"):
            wait_ticket(submit_connect(runtime, time.monotonic() + 1))
    finally:
        close_actor(runtime)


def test_connect_deadline_retires_generation() -> None:
    created: list[StalledSocket] = []

    def factory(family: int, socktype: int, proto: int) -> StalledSocket:
        sock = StalledSocket(family, socktype, proto)
        created.append(sock)
        return sock

    runtime = start_actor(8, resolve_hosts(["127.0.0.1:9"]), socket_factory=factory)
    try:
        ticket = submit_connect(runtime, time.monotonic() + 0.02)
        with pytest.raises(Exception, match="timed out during connect"):
            wait_ticket(ticket)
        assert ticket.completed.wait(timeout=1)
        assert runtime.generation is None
        assert runtime.generation_counter == 1
        assert created[0].so_error_calls <= 4
    finally:
        close_actor(runtime)


def test_wakeup_full_is_coalesced_and_writer_eof_is_fatal() -> None:
    class FullWriter:
        def send(self, data: bytes) -> int:
            raise BlockingIOError

    runtime = actor_module.ActorRuntime(runtime_epoch=9, endpoints=())
    runtime.wake_writer = FullWriter()
    actor_module._notify_actor(runtime)

    running = start_actor(10, resolve_hosts(["127.0.0.1:9"]))
    writer = running.wake_writer
    assert writer is not None
    writer.close()
    assert running.stopped.wait(timeout=2)
    assert running.state is RuntimeState.FAILED
    assert isinstance(running.fatal_error, ConnectionClosedError)
    assert running.selector is None
    assert running.wake_reader is None


def test_many_submitters_never_overwrite_the_single_mailbox() -> None:
    runtime = start_actor(11, resolve_hosts(["127.0.0.1:9"]), socket_factory=ImmediateSocket)
    barrier = threading.Barrier(51)
    tickets = []
    errors = []
    result_lock = threading.Lock()

    def submit() -> None:
        barrier.wait(timeout=2)
        try:
            ticket = submit_connect(runtime, time.monotonic() + 2)
        except BaseException as exc:
            with result_lock:
                errors.append(exc)
        else:
            with result_lock:
                tickets.append(ticket)

    threads = [threading.Thread(target=submit) for _ in range(50)]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=2)
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()
    try:
        assert tickets
        for ticket in tickets:
            assert wait_ticket(ticket) == "127.0.0.1:9"
        assert all("mailbox is full" in str(exc) for exc in errors)
        assert actor_snapshot(runtime).pending_depth == 0
    finally:
        close_actor(runtime)


def test_wire_request_auto_handshakes_and_returns_envelope_for_caller_parser() -> None:
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_SECURITY_COUNT
        raw = response_bytes(msg_id, msg_type, (321).to_bytes(2, "little"))
        for value in raw:
            conn.sendall(bytes((value,)))
        release.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(12, resolve_hosts([server.host]))
        try:
            ticket = submit_request(
                runtime,
                lease_id=44,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 2,
                retry_safe=True,
            )
            envelope = wait_ticket(ticket)
            assert isinstance(envelope, FrameEnvelope)
            assert (envelope.runtime_epoch, envelope.tcp_generation, envelope.lease_id) == (12, 1, 44)
            assert envelope.msg_type == TYPE_SECURITY_COUNT
            assert parse_command_response(envelope.command, envelope.response, envelope.request_payload_snapshot) == 321
            assert runtime.last_handshake is not None
        finally:
            release.set()
            close_actor(runtime)


def test_retry_safe_eof_uses_new_generation_and_ignores_old_identity_frame() -> None:
    release = threading.Event()
    first_request_id = 0

    def first(conn: socket.socket) -> None:
        nonlocal first_request_id
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        first_request_id, msg_type, _ = read_request(conn)

    def second(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(first_request_id, msg_type, (111).to_bytes(2, "little")))
        conn.sendall(response_bytes(msg_id, msg_type, (777).to_bytes(2, "little")))
        release.wait(timeout=2)

    with Scripted7709Server([first, second]) as server:
        runtime = start_actor(13, resolve_hosts([server.host]))
        try:
            ticket = submit_request(
                runtime,
                lease_id=45,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 2,
                retry_safe=True,
            )
            envelope = wait_ticket(ticket)
            assert parse_command_response(envelope.command, envelope.response, envelope.request_payload_snapshot) == 777
            assert ticket.attempts == 2
            assert envelope.tcp_generation == 2
            assert runtime.stale_event_count == 1
        finally:
            release.set()
            close_actor(runtime)


def test_non_retry_safe_request_does_not_replay_after_sending_bytes() -> None:
    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        read_request(conn)

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(17, resolve_hosts([server.host]))
        try:
            ticket = submit_request(
                runtime,
                lease_id=49,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 1,
                retry_safe=False,
            )
            with pytest.raises(ConnectionClosedError, match="remote peer"):
                wait_ticket(ticket)
            assert ticket.attempts == 1
            assert runtime.generation_counter == 1
        finally:
            close_actor(runtime)


def test_cancel_after_send_retires_generation_and_completes_once() -> None:
    request_received = threading.Event()
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        read_request(conn)
        request_received.set()
        release.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(14, resolve_hosts([server.host]))
        ticket = submit_request(
            runtime,
            lease_id=46,
            command=TYPE_SECURITY_COUNT,
            payload={"market": "sz"},
            deadline=time.monotonic() + 2,
            retry_safe=True,
        )
        try:
            assert request_received.wait(timeout=2)
            cancel_ticket(runtime, ticket)
            with pytest.raises(ConnectionClosedError, match="cancelled"):
                wait_ticket(ticket)
            assert runtime.generation is None
        finally:
            release.set()
            close_actor(runtime)


def test_response_timeout_reports_stage_and_retires_generation() -> None:
    request_received = threading.Event()
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        read_request(conn)
        request_received.set()
        release.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(15, resolve_hosts([server.host]))
        ticket = submit_request(
            runtime,
            lease_id=47,
            command=TYPE_SECURITY_COUNT,
            payload={"market": "sz"},
            deadline=time.monotonic() + 0.2,
            retry_safe=False,
        )
        try:
            assert request_received.wait(timeout=2)
            with pytest.raises(ResponseTimeoutError, match="during response"):
                wait_ticket(ticket)
            assert ticket.completed.wait(timeout=1)
            assert runtime.generation is None
        finally:
            release.set()
            close_actor(runtime)


def test_terminal_hook_observes_success_before_cancel_close_and_caller_wakeup() -> None:
    terminal = threading.Event()
    publish = threading.Event()
    release_server = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (9).to_bytes(2, "little")))
        release_server.wait(timeout=2)

    def completion(ticket) -> None:
        assert ticket.state.name == "SUCCESS"
        assert not ticket.completed.is_set()
        terminal.set()
        assert publish.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(16, resolve_hosts([server.host]))
        ticket = submit_request(
            runtime,
            lease_id=48,
            command=TYPE_SECURITY_COUNT,
            payload={"market": "sz"},
            deadline=time.monotonic() + 2,
            retry_safe=True,
            completion=completion,
        )
        try:
            assert terminal.wait(timeout=2)
            cancel_ticket(runtime, ticket)
            request_actor_stop(runtime)
            publish.set()
            envelope = wait_ticket(ticket)
            assert parse_command_response(envelope.command, envelope.response, envelope.request_payload_snapshot) == 9
        finally:
            publish.set()
            release_server.set()
            close_actor(runtime)


class PartialSocket:
    def __init__(self, outcomes) -> None:
        self.outcomes = list(outcomes)

    def send(self, data) -> int:
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return min(outcome, len(data))


class InterestSelector:
    def __init__(self) -> None:
        self.events = []

    def modify(self, sock, events, data) -> None:
        self.events.append(events)


def test_partial_send_preserves_every_offset_and_blocking_error() -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    ticket = actor_module.RequestTicket(1, 1, TYPE_SECURITY_COUNT, {}, time.monotonic() + 1, True)
    sock = PartialSocket([BlockingIOError(), 1, 2, 3])
    generation = actor_module.TcpGeneration(1, sock, endpoint, TcpState.READY)
    generation.tx_bytes = b"abcdef"
    generation.active_exchange = actor_module.WireExchange(ticket, TYPE_SECURITY_COUNT, 1, TYPE_SECURITY_COUNT, b"abcdef", False)
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.selector = InterestSelector()
    runtime.generation = generation
    runtime.active_task = ticket

    observed = []
    for _ in range(4):
        actor_module._send_generation(runtime, generation)
        observed.append(generation.tx_offset)

    assert observed == [0, 1, 3, 6]
    assert generation.active_exchange.sent_any
    assert runtime.selector.events == [
        actor_module.selectors.EVENT_READ | actor_module.selectors.EVENT_WRITE,
        actor_module.selectors.EVENT_READ,
    ]


def test_immediate_full_send_keeps_existing_read_interest() -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    ticket = actor_module.RequestTicket(1, 1, TYPE_SECURITY_COUNT, {"market": "sz"}, time.monotonic() + 1, True)
    sock = PartialSocket([1_000_000])
    generation = actor_module.TcpGeneration(1, sock, endpoint, TcpState.READY)
    generation.selector_events = actor_module.selectors.EVENT_READ
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.selector = InterestSelector()
    runtime.generation = generation
    runtime.active_task = ticket

    assert actor_module._begin_exchange(runtime, ticket, TYPE_SECURITY_COUNT, handshake=False)

    assert generation.tx_offset == len(generation.tx_bytes)
    assert ticket.state is actor_module.RequestState.WAITING_RESPONSE
    assert runtime.selector.events == []


def test_send_zero_is_connection_closed_and_old_socket_token_is_stale() -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    ticket = actor_module.RequestTicket(1, 1, TYPE_SECURITY_COUNT, {}, time.monotonic() + 1, True)
    sock = PartialSocket([0])
    generation = actor_module.TcpGeneration(1, sock, endpoint, TcpState.READY)
    generation.tx_bytes = b"abc"
    generation.active_exchange = actor_module.WireExchange(ticket, TYPE_SECURITY_COUNT, 1, TYPE_SECURITY_COUNT, b"abc", False)
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.generation = generation
    runtime.active_task = ticket
    with pytest.raises(ConnectionClosedError, match="during send"):
        actor_module._send_generation(runtime, generation)

    old_token = actor_module.SelectorToken("tcp", 1, 1, object())
    actor_module._handle_tcp_event(runtime, old_token, actor_module.selectors.EVENT_WRITE)
    assert runtime.stale_event_count == 1


def test_close_during_connect_is_woken_and_only_actor_closes_tcp_socket() -> None:
    created: list[StalledSocket] = []

    def factory(family: int, socktype: int, proto: int) -> StalledSocket:
        item = StalledSocket(family, socktype, proto)
        created.append(item)
        return item

    runtime = start_actor(3, resolve_hosts(["127.0.0.1:9"]), socket_factory=factory)
    ticket = submit_connect(runtime, time.monotonic() + 10)
    assert runtime.generation_started.wait(timeout=2)

    started = time.perf_counter()
    close_actor(runtime)

    assert time.perf_counter() - started < 0.1
    with pytest.raises(ConnectionClosedError, match="Actor stopped"):
        wait_ticket(ticket)
    assert created[0].closed_by == "eltdx-7709-actor-3"


def test_idle_actor_close_wakes_blocked_selector_without_polling() -> None:
    runtime = start_actor(4, resolve_hosts(["127.0.0.1:9"]))

    started = time.perf_counter()
    close_actor(runtime)

    assert time.perf_counter() - started < 0.1
    assert runtime.state is RuntimeState.STOPPED


def test_mailbox_capacity_is_one() -> None:
    runtime = start_actor(5, resolve_hosts(["127.0.0.1:9"]), socket_factory=StalledSocket)
    try:
        submit_connect(runtime, time.monotonic() + 10)
        assert runtime.generation_started.wait(timeout=2)
        submit_connect(runtime, time.monotonic() + 10)
        with pytest.raises(Exception, match="mailbox is full"):
            submit_connect(runtime, time.monotonic() + 10)
    finally:
        close_actor(runtime)
