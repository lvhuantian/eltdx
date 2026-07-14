from __future__ import annotations

import errno
import socket
import sys
import threading
import time
from collections.abc import Callable

import pytest

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx.exceptions import ResponseTimeoutError
from eltdx.hosts import resolve_hosts
from eltdx.protocol.commands import parse_command_response
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_SECURITY_COUNT
from eltdx.transport import actor as actor_module
from eltdx.transport.actor import close_actor, start_actor, submit_connect, submit_request, wait_ticket


def _healthy_handler(value: int, release: threading.Event) -> Callable[[socket.socket], None]:
    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_SECURITY_COUNT
        conn.sendall(response_bytes(msg_id, msg_type, value.to_bytes(2, "little")))
        assert release.wait(timeout=2)

    return handler


def _execute_count(runtime, timeout: float = 1.0) -> int:
    ticket = submit_request(
        runtime,
        lease_id=0,
        command=TYPE_SECURITY_COUNT,
        payload={"market": "sz"},
        deadline=time.monotonic() + timeout,
        retry_safe=True,
    )
    envelope = wait_ticket(ticket)
    return parse_command_response(envelope.command, envelope.response, envelope.request_payload_snapshot)


def test_handshake_eof_retry_starts_next_real_loopback_host() -> None:
    release = threading.Event()

    def handshake_eof(conn: socket.socket) -> None:
        _, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE

    with Scripted7709Server([handshake_eof]) as bad, Scripted7709Server(
        [_healthy_handler(777, release)]
    ) as healthy:
        runtime = start_actor(201, resolve_hosts([bad.host, healthy.host]))
        actor_thread = runtime.actor_thread
        try:
            assert _execute_count(runtime) == 777
            assert bad.accepted_count == 1
            assert healthy.accepted_count == 1
            assert runtime.actor_thread is actor_thread and actor_thread is not None and actor_thread.is_alive()
            assert (runtime.generation_counter, runtime.reconnect_count) == (2, 1)
        finally:
            release.set()
            close_actor(runtime)


def test_handshake_attempt_timeout_retries_next_real_loopback_host() -> None:
    bad_received = threading.Event()
    release_bad = threading.Event()
    release_healthy = threading.Event()

    def handshake_stall(conn: socket.socket) -> None:
        _, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        bad_received.set()
        assert release_bad.wait(timeout=2)

    with Scripted7709Server([handshake_stall]) as bad, Scripted7709Server(
        [_healthy_handler(781, release_healthy)]
    ) as healthy:
        runtime = start_actor(207, resolve_hosts([bad.host, healthy.host]))
        actor_thread = runtime.actor_thread
        started = time.monotonic()
        try:
            assert _execute_count(runtime) == 781
            assert bad_received.is_set()
            assert time.monotonic() - started < 1.0
            assert bad.accepted_count == 1
            assert healthy.accepted_count == 1
            assert runtime.actor_thread is actor_thread and actor_thread is not None and actor_thread.is_alive()
            assert (runtime.generation_counter, runtime.reconnect_count) == (2, 1)
        finally:
            release_bad.set()
            release_healthy.set()
            close_actor(runtime)


def test_business_eof_retry_starts_next_real_loopback_host() -> None:
    release = threading.Event()

    def business_eof(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        _, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_SECURITY_COUNT

    with Scripted7709Server([business_eof]) as bad, Scripted7709Server(
        [_healthy_handler(778, release)]
    ) as healthy:
        runtime = start_actor(202, resolve_hosts([bad.host, healthy.host]))
        actor_thread = runtime.actor_thread
        try:
            assert _execute_count(runtime) == 778
            assert bad.accepted_count == 1
            assert healthy.accepted_count == 1
            assert runtime.actor_thread is actor_thread and actor_thread is not None and actor_thread.is_alive()
            assert (runtime.generation_counter, runtime.reconnect_count) == (2, 1)
        finally:
            release.set()
            close_actor(runtime)


class FailBusinessSendSocket:
    def __init__(self, family: int, socktype: int, proto: int) -> None:
        self._socket = socket.socket(family, socktype, proto)
        self._fail_business_continuation = False

    def send(self, data) -> int:
        if self._fail_business_continuation:
            raise ConnectionResetError("injected partial business send failure")
        view = bytes(data)
        if len(view) >= 12 and int.from_bytes(view[10:12], "little") == TYPE_SECURITY_COUNT:
            sent = self._socket.send(memoryview(data)[:1])
            self._fail_business_continuation = True
            return sent
        return self._socket.send(data)

    def __getattr__(self, name: str):
        return getattr(self._socket, name)


def test_business_send_failure_retries_next_real_loopback_host() -> None:
    release = threading.Event()
    first_socket = True

    def factory(family: int, socktype: int, proto: int):
        nonlocal first_socket
        if first_socket:
            first_socket = False
            return FailBusinessSendSocket(family, socktype, proto)
        return socket.socket(family, socktype, proto)

    def handshake_only(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        assert conn.recv(1) == b"\x0c"
        assert conn.recv(1) == b""

    with Scripted7709Server([handshake_only]) as bad, Scripted7709Server(
        [_healthy_handler(780, release)]
    ) as healthy:
        runtime = start_actor(206, resolve_hosts([bad.host, healthy.host]), socket_factory=factory)
        actor_thread = runtime.actor_thread
        try:
            assert _execute_count(runtime) == 780
            assert bad.accepted_count == 1
            assert healthy.accepted_count == 1
            assert runtime.actor_thread is actor_thread and actor_thread is not None and actor_thread.is_alive()
            assert (runtime.generation_counter, runtime.reconnect_count) == (2, 1)
        finally:
            release.set()
            close_actor(runtime)


def test_response_attempt_timeout_retries_next_host_within_absolute_deadline() -> None:
    bad_received = threading.Event()
    release_bad = threading.Event()
    release_healthy = threading.Event()

    def no_response(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        read_request(conn)
        bad_received.set()
        assert release_bad.wait(timeout=2)

    with Scripted7709Server([no_response]) as bad, Scripted7709Server(
        [_healthy_handler(779, release_healthy)]
    ) as healthy:
        runtime = start_actor(203, resolve_hosts([bad.host, healthy.host]))
        actor_thread = runtime.actor_thread
        started = time.monotonic()
        try:
            assert _execute_count(runtime, timeout=1.0) == 779
            assert bad_received.is_set()
            assert time.monotonic() - started < 1.0
            assert bad.accepted_count == 1
            assert healthy.accepted_count == 1
            assert runtime.actor_thread is actor_thread and actor_thread is not None and actor_thread.is_alive()
            assert (runtime.generation_counter, runtime.reconnect_count) == (2, 1)
        finally:
            release_bad.set()
            release_healthy.set()
            close_actor(runtime)


def test_all_failed_hosts_share_one_absolute_deadline() -> None:
    releases = [threading.Event(), threading.Event()]
    received = [threading.Event(), threading.Event()]

    def no_response(index: int) -> Callable[[socket.socket], None]:
        def handler(conn: socket.socket) -> None:
            msg_id, msg_type, _ = read_request(conn)
            conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
            read_request(conn)
            received[index].set()
            assert releases[index].wait(timeout=2)

        return handler

    timeout = 0.6
    with Scripted7709Server([no_response(0)]) as first, Scripted7709Server([no_response(1)]) as second:
        runtime = start_actor(204, resolve_hosts([first.host, second.host]))
        started = time.monotonic()
        try:
            with pytest.raises(ResponseTimeoutError, match="during response"):
                _execute_count(runtime, timeout=timeout)
            elapsed = time.monotonic() - started
            assert timeout * 0.8 <= elapsed <= timeout + 0.1
            assert all(event.is_set() for event in received)
            assert (first.accepted_count, second.accepted_count) == (1, 1)
        finally:
            for event in releases:
                event.set()
            close_actor(runtime)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows connect_ex regression")
def test_windows_closed_first_endpoint_reaches_healthy_before_shared_deadline() -> None:
    assert errno.WSAEINTR in actor_module._IN_PROGRESS_CONNECT_CODES
    closed = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    closed.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    closed.bind(("127.0.0.1", 0))
    address, port = closed.getsockname()
    release = threading.Event()

    def healthy_handler(conn: socket.socket) -> None:
        assert release.wait(timeout=2)

    with Scripted7709Server([healthy_handler]) as healthy:
        runtime = start_actor(205, resolve_hosts([f"{address}:{port}", healthy.host]))
        started = time.monotonic()
        try:
            assert wait_ticket(submit_connect(runtime, time.monotonic() + 1.0)) == healthy.host
            assert time.monotonic() - started < 1.0
            assert healthy.wait_for_connections(1)
        finally:
            release.set()
            close_actor(runtime)
            closed.close()
