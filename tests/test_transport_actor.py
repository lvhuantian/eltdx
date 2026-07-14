from __future__ import annotations

import errno
import socket
import threading
import time

import pytest

from actor_support import Scripted7709Server
from eltdx.exceptions import ConnectionClosedError
from eltdx.hosts import resolve_host, resolve_hosts
from eltdx.transport.actor import (
    RuntimeState,
    TcpState,
    actor_snapshot,
    close_actor,
    start_actor,
    submit_connect,
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

    def setblocking(self, value: bool) -> None:
        self._socket.setblocking(value)

    def connect_ex(self, address) -> int:
        return errno.EINPROGRESS

    def getsockopt(self, level: int, option: int) -> int:
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
    runtime = start_actor(8, resolve_hosts(["127.0.0.1:9"]), socket_factory=StalledSocket)
    try:
        ticket = submit_connect(runtime, time.monotonic() + 0.02)
        with pytest.raises(Exception, match="timed out during connect"):
            wait_ticket(ticket)
        assert runtime.generation is None
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
