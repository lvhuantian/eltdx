from __future__ import annotations

import gc
import socket
import threading
import time
import weakref
from dataclasses import FrozenInstanceError

import pytest

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx import TdxClient
from eltdx.exceptions import ConnectionClosedError, TransportCloseTimeoutError
from eltdx.hosts import resolve_host, resolve_hosts
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_HEARTBEAT, TYPE_SECURITY_COUNT
from eltdx.transport import PooledSocketTransport, SocketTransport
from eltdx.transport import pool as pool_module
from eltdx.transport import socket as socket_module
from eltdx.transport.actor import (
    ActorRuntime,
    RuntimeState,
    abandon_actor,
    submit_request,
)
from eltdx.transport.pool import LeaseCompletion, PoolState
from eltdx.transport.push import PushBuffer


def test_normal_close_reopens_with_new_runtime_and_old_stop_cannot_touch_it() -> None:
    release = threading.Event()

    def handler(value: int):
        def serve(conn: socket.socket) -> None:
            msg_id, msg_type, _ = read_request(conn)
            conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
            msg_id, msg_type, _ = read_request(conn)
            conn.sendall(response_bytes(msg_id, msg_type, value.to_bytes(2, "little")))
            release.wait(timeout=2)

        return serve

    with Scripted7709Server([handler(1), handler(2)]) as server:
        transport = SocketTransport(hosts=[server.host], timeout=2, heartbeat_interval=None)
        try:
            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 1
            old_runtime = transport._runtime
            assert old_runtime is not None
            transport.close()
            assert old_runtime.state is RuntimeState.STOPPED

            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 2
            new_runtime = transport._runtime
            assert new_runtime is not None and new_runtime is not old_runtime
            abandon_actor(old_runtime)
            assert new_runtime.actor_thread is not None and new_runtime.actor_thread.is_alive()
            diagnostics = transport.diagnostics
            assert diagnostics.actor is not None and diagnostics.actor.runtime_epoch == new_runtime.runtime_epoch
            with pytest.raises(FrozenInstanceError):
                diagnostics.epoch = 0  # type: ignore[misc]
        finally:
            release.set()
            transport.close()


def test_close_timeout_retains_runtime_then_becomes_failed_closed_without_reopen() -> None:
    release = threading.Event()
    runtime = ActorRuntime(runtime_epoch=99, endpoints=())
    runtime.state = RuntimeState.RUNNING
    thread = threading.Thread(target=lambda: release.wait(timeout=2), name="stalled-actor-test", daemon=True)
    runtime.actor_thread = thread
    thread.start()
    transport = SocketTransport(hosts=["127.0.0.1:9"], timeout=0.1, heartbeat_interval=None)
    transport._runtime = runtime
    transport._push_buffer = PushBuffer(99)
    transport._epoch = 99

    with pytest.raises(TransportCloseTimeoutError):
        transport._close_with_timeout(0.02)
    assert transport._runtime is runtime
    assert runtime.state is RuntimeState.FAILED_CLOSING

    release.set()
    thread.join(timeout=2)
    transport.close()
    assert transport._runtime is runtime
    assert runtime.state is RuntimeState.FAILED_CLOSED
    with pytest.raises(ConnectionClosedError, match="not usable"):
        transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"})


def test_standalone_actor_fatal_is_visible_and_failed_closed() -> None:
    transport = SocketTransport(hosts=["127.0.0.1:9"], timeout=0.2, heartbeat_interval=None)
    runtime = transport._ensure_runtime()
    writer = runtime.wake_writer
    assert writer is not None
    writer.close()

    assert runtime.stopped.wait(timeout=2)
    assert runtime.state is RuntimeState.FAILED
    assert transport.diagnostics.actor is not None
    with pytest.raises(ConnectionClosedError, match="wakeup writer closed"):
        transport.poll_push()
    with pytest.raises(ConnectionClosedError, match="not usable"):
        transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"})

    transport.close()
    assert runtime.state is RuntimeState.FAILED_CLOSED
    assert transport._runtime is runtime


def test_pool_actor_fatal_stops_siblings_and_fails_closed() -> None:
    pool = PooledSocketTransport(hosts=["127.0.0.1:9"], timeout=0.2, pool_size=2, heartbeat_interval=None)
    pool._ensure_started()
    runtimes = [transport._ensure_runtime() for transport in pool._transports]
    writer = runtimes[0].wake_writer
    assert writer is not None
    writer.close()

    assert all(runtime.stopped.wait(timeout=2) for runtime in runtimes)
    assert pool.diagnostics.state is PoolState.FAILED
    assert pool.diagnostics.broker is not None and pool.diagnostics.broker.closed
    with pytest.raises(ConnectionClosedError):
        pool.poll_push()
    with pytest.raises(ConnectionClosedError, match="Actor terminated"):
        pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"})

    pool.close()
    assert pool.diagnostics.state is PoolState.FAILED_CLOSED


def test_standalone_finalizer_stops_connected_runtime() -> None:
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        release.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        transport = SocketTransport(hosts=[server.host], timeout=1, heartbeat_interval=None)
        transport.connect()
        runtime = transport._runtime
        reference = weakref.ref(transport)
        assert runtime is not None

        del transport
        gc.collect()

        assert reference() is None
        assert runtime.stopped.wait(timeout=2)
        release.set()


def test_standalone_finalizer_stops_idle_actor() -> None:
    transport = SocketTransport(hosts=["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    runtime = transport._ensure_runtime()
    reference = weakref.ref(transport)

    del transport
    gc.collect()

    assert reference() is None
    assert runtime.stopped.wait(timeout=2)


def test_standalone_finalizer_cancels_waiting_request() -> None:
    request_received = threading.Event()
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        read_request(conn)
        request_received.set()
        release.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        transport = SocketTransport(hosts=[server.host], timeout=2, heartbeat_interval=None)
        runtime = transport._ensure_runtime()
        ticket = submit_request(
            runtime,
            lease_id=0,
            command=TYPE_SECURITY_COUNT,
            payload={"market": "sz"},
            deadline=time.monotonic() + 2,
            retry_safe=True,
        )
        assert request_received.wait(timeout=2)
        reference = weakref.ref(transport)

        del transport
        gc.collect()

        assert reference() is None
        assert runtime.stopped.wait(timeout=2)
        assert ticket.completed.is_set() and isinstance(ticket.error, ConnectionClosedError)
        release.set()


def test_pooled_client_finalizer_stops_all_connected_runtimes() -> None:
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        release.wait(timeout=2)

    with Scripted7709Server([handler, handler]) as server:
        pool = PooledSocketTransport(hosts=[server.host], timeout=1, pool_size=2, heartbeat_interval=None)
        pool.connect()
        runtimes = [transport._runtime for transport in pool._transports]
        client = TdxClient(transport=pool)
        pool_reference = weakref.ref(pool)

        del client
        del pool
        gc.collect()

        assert pool_reference() is None
        assert all(runtime is not None and runtime.stopped.wait(timeout=2) for runtime in runtimes)
        release.set()


def test_pool_finalizer_closes_idle_broker_and_push_buffer() -> None:
    pool = PooledSocketTransport(hosts=["127.0.0.1:9"], timeout=1, pool_size=2, heartbeat_interval=None)
    broker, push_buffer = pool._ensure_started()
    reference = weakref.ref(pool)

    del pool
    gc.collect()

    assert reference() is None
    assert broker.snapshot().closed
    assert push_buffer.snapshot().closed


def test_pool_finalizer_cancels_detached_waiting_request() -> None:
    request_received = threading.Event()
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        read_request(conn)
        request_received.set()
        release.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport(hosts=[server.host], timeout=2, pool_size=1, heartbeat_interval=None)
        broker, _ = pool._ensure_started()
        lease = broker.acquire(time.monotonic() + 2)
        slot = pool._transports[lease.slot_id]
        runtime = slot._ensure_runtime()
        ticket = submit_request(
            runtime,
            lease_id=lease.lease_id,
            command=TYPE_SECURITY_COUNT,
            payload={"market": "sz"},
            deadline=time.monotonic() + 2,
            retry_safe=True,
            completion=LeaseCompletion(broker, lease),
        )
        assert request_received.wait(timeout=2)
        reference = weakref.ref(pool)

        del pool
        gc.collect()

        assert reference() is None
        assert runtime.stopped.wait(timeout=2)
        assert ticket.completed.is_set() and isinstance(ticket.error, ConnectionClosedError)
        assert broker.snapshot().active_leases == 0
        release.set()
        del slot


def test_pool_close_stops_heartbeat_without_reconnect() -> None:
    heartbeat_interval = 0.02

    def handler(conn: socket.socket) -> None:
        while True:
            try:
                msg_id, msg_type, _ = read_request(conn)
            except (EOFError, OSError):
                return
            if msg_type == TYPE_HANDSHAKE:
                payload = handshake_payload()
            elif msg_type == TYPE_HEARTBEAT:
                payload = bytes.fromhex("0000000000008f173501")
            else:
                raise AssertionError(f"unexpected request type: {msg_type:#x}")
            conn.sendall(response_bytes(msg_id, msg_type, payload))

    with Scripted7709Server([handler] * 8) as server:
        pool = PooledSocketTransport(
            [server.host],
            pool_size=2,
            timeout=1,
            heartbeat_interval=heartbeat_interval,
        )
        pool.connect()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not all(
            slot.last_heartbeat is not None for slot in pool._transports
        ):
            threading.Event().wait(0.01)
        assert all(slot.last_heartbeat is not None for slot in pool._transports)
        runtimes = [slot._runtime for slot in pool._transports]
        assert all(runtime is not None for runtime in runtimes)

        pool.close()
        accepted_after_close = server.accepted_count
        assert all(
            runtime is not None
            and runtime.stopped.is_set()
            and runtime.actor_thread is not None
            and not runtime.actor_thread.is_alive()
            for runtime in runtimes
        )
        threading.Event().wait(heartbeat_interval * 2.5)

        assert server.accepted_count == accepted_after_close


@pytest.mark.parametrize("operation", ["connect", "execute"])
def test_standalone_public_deadline_starts_after_dns_preflight(monkeypatch, operation: str) -> None:
    endpoints = resolve_hosts(["127.0.0.1:9"])
    events: list[str] = []
    clock = [10.0]
    deadlines: list[float] = []
    runtime = object()

    def resolve(_hosts):
        events.append("resolve")
        clock[0] = 40.0
        return endpoints

    def monotonic() -> float:
        events.append("deadline")
        return clock[0]

    transport = SocketTransport(hosts=["custom.invalid:7709"], timeout=3, heartbeat_interval=None)
    monkeypatch.setattr(socket_module, "resolve_hosts", resolve)
    monkeypatch.setattr(socket_module.time, "monotonic", monotonic)
    monkeypatch.setattr(
        transport,
        "_ensure_runtime",
        lambda deadline, **_kwargs: deadlines.append(deadline) or runtime,
    )
    monkeypatch.setattr(transport, "_connect_with_deadline", lambda **_kwargs: None)
    monkeypatch.setattr(transport, "_execute_with_lease", lambda *_args, **_kwargs: 1)

    if operation == "connect":
        transport.connect()
    else:
        assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 1

    assert events == ["resolve", "deadline"]
    assert deadlines == [43.0]


def test_concurrent_standalone_calls_share_one_dns_preflight(monkeypatch) -> None:
    entered = threading.Event()
    waiter_entered = threading.Event()
    release = threading.Event()
    endpoints = resolve_hosts(["127.0.0.1:9"])
    resolve_calls = 0
    results: list[object] = []

    class ObservedCondition(threading.Condition):
        def wait(self, timeout=None):
            waiter_entered.set()
            return super().wait(timeout)

    def resolve(_hosts):
        nonlocal resolve_calls
        resolve_calls += 1
        entered.set()
        assert release.wait(timeout=2)
        return endpoints

    transport = SocketTransport(hosts=["custom.invalid:7709"], timeout=1, heartbeat_interval=None)
    transport._lifecycle = ObservedCondition()
    monkeypatch.setattr(socket_module, "resolve_hosts", resolve)
    monkeypatch.setattr(transport, "_ensure_runtime", lambda _deadline, **_kwargs: object())
    monkeypatch.setattr(transport, "_connect_with_deadline", lambda **_kwargs: None)
    callers = [threading.Thread(target=lambda: _capture(results, transport.connect)) for _ in range(2)]

    for caller in callers:
        caller.start()
    assert entered.wait(timeout=2)
    assert waiter_entered.wait(timeout=2)
    release.set()
    for caller in callers:
        caller.join(timeout=2)

    assert all(not caller.is_alive() for caller in callers)
    assert results == [None, None]
    assert resolve_calls == 1
    assert transport._resolved_endpoints == endpoints


def test_close_during_dns_preflight_discards_result_before_actor_start(monkeypatch) -> None:
    entered = threading.Event()
    release = threading.Event()
    endpoints = resolve_hosts(["127.0.0.1:9"])
    actor_starts: list[object] = []

    def resolve(hosts):
        entered.set()
        assert release.wait(timeout=2)
        return endpoints

    monkeypatch.setattr(socket_module, "resolve_hosts", resolve)
    monkeypatch.setattr(socket_module, "start_actor", lambda *args, **kwargs: actor_starts.append((args, kwargs)))
    transport = SocketTransport(hosts=["custom.invalid:7709"], timeout=1, heartbeat_interval=None)
    result = []
    starter = threading.Thread(target=lambda: _capture(result, transport.connect))
    starter.start()
    assert entered.wait(timeout=2)
    transport.close()
    release.set()
    starter.join(timeout=2)

    assert not starter.is_alive()
    assert transport._runtime is None and transport._candidate is None
    assert transport._resolved_endpoints is None and transport._resolver_claim is None
    assert actor_starts == []
    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)


def test_close_after_dns_preflight_rejects_original_public_call(monkeypatch) -> None:
    clock_entered = threading.Event()
    release_clock = threading.Event()
    endpoints = resolve_hosts(["127.0.0.1:9"])
    actor_starts: list[object] = []
    results: list[object] = []
    real_monotonic = time.monotonic

    def monotonic() -> float:
        if threading.current_thread().name == "pre-close-connect":
            clock_entered.set()
            assert release_clock.wait(timeout=2)
        return real_monotonic()

    transport = SocketTransport(hosts=["custom.invalid:7709"], timeout=1, heartbeat_interval=None)
    monkeypatch.setattr(socket_module, "resolve_hosts", lambda _hosts: endpoints)
    monkeypatch.setattr(socket_module.time, "monotonic", monotonic)
    monkeypatch.setattr(socket_module, "start_actor", lambda *args, **kwargs: actor_starts.append((args, kwargs)))
    caller = threading.Thread(
        target=lambda: _capture(results, transport.connect),
        name="pre-close-connect",
    )

    caller.start()
    assert clock_entered.wait(timeout=2)
    transport.close()
    release_clock.set()
    caller.join(timeout=2)

    assert not caller.is_alive()
    assert len(results) == 1 and isinstance(results[0], ConnectionClosedError)
    assert actor_starts == []
    assert transport._runtime is None and transport._candidate is None


def test_reopen_dns_does_not_wait_for_old_generation_resolver(monkeypatch) -> None:
    old_entered = threading.Event()
    allow_old = threading.Event()
    old_endpoints = resolve_hosts(["127.0.0.1:9"])
    new_endpoints = resolve_hosts(["127.0.0.1:10"])
    resolve_calls = 0
    resolve_lock = threading.Lock()
    old_results: list[object] = []
    new_results: list[object] = []

    def resolve(_hosts):
        nonlocal resolve_calls
        with resolve_lock:
            resolve_calls += 1
            call = resolve_calls
        if call == 1:
            old_entered.set()
            assert allow_old.wait(timeout=2)
            return old_endpoints
        assert call == 2
        return new_endpoints

    transport = SocketTransport(hosts=["custom.invalid:7709"], timeout=1, heartbeat_interval=None)

    def ensure(_deadline, *, expected_close_generation, **_kwargs):
        with transport._lifecycle:
            if expected_close_generation != transport._close_generation:
                raise ConnectionClosedError("stale public operation")
        return object()

    monkeypatch.setattr(socket_module, "resolve_hosts", resolve)
    monkeypatch.setattr(transport, "_ensure_runtime", ensure)
    monkeypatch.setattr(transport, "_connect_with_deadline", lambda **_kwargs: None)
    old_caller = threading.Thread(target=lambda: _capture(old_results, transport.connect))
    new_caller = threading.Thread(target=lambda: _capture(new_results, transport.connect))

    old_caller.start()
    assert old_entered.wait(timeout=2)
    transport.close()
    new_caller.start()
    new_caller.join(timeout=2)
    assert not new_caller.is_alive()
    assert new_results == [None]
    assert transport._resolved_endpoints == new_endpoints

    allow_old.set()
    old_caller.join(timeout=2)

    assert not old_caller.is_alive()
    assert len(old_results) == 1 and isinstance(old_results[0], ConnectionClosedError)
    assert resolve_calls == 2
    assert transport._resolved_endpoints == new_endpoints
    assert transport._resolver_claim is None


def test_failed_dns_preflight_wakes_waiter_and_allows_retry(monkeypatch) -> None:
    first_entered = threading.Event()
    waiter_entered = threading.Event()
    release_failure = threading.Event()
    endpoints = resolve_hosts(["127.0.0.1:9"])
    resolve_calls = 0
    results: list[object] = []

    class ObservedCondition(threading.Condition):
        def wait(self, timeout=None):
            waiter_entered.set()
            return super().wait(timeout)

    def resolve(_hosts):
        nonlocal resolve_calls
        resolve_calls += 1
        if resolve_calls == 1:
            first_entered.set()
            assert release_failure.wait(timeout=2)
            raise OSError("deterministic DNS failure")
        return endpoints

    transport = SocketTransport(hosts=["custom.invalid:7709"], timeout=1, heartbeat_interval=None)
    transport._lifecycle = ObservedCondition()
    monkeypatch.setattr(socket_module, "resolve_hosts", resolve)
    monkeypatch.setattr(transport, "_ensure_runtime", lambda _deadline, **_kwargs: object())
    monkeypatch.setattr(transport, "_connect_with_deadline", lambda **_kwargs: None)
    callers = [threading.Thread(target=lambda: _capture(results, transport.connect)) for _ in range(2)]

    for caller in callers:
        caller.start()
    assert first_entered.wait(timeout=2)
    assert waiter_entered.wait(timeout=2)
    release_failure.set()
    for caller in callers:
        caller.join(timeout=2)

    assert all(not caller.is_alive() for caller in callers)
    assert len(results) == 2
    assert sum(isinstance(result, ConnectionClosedError) for result in results) == 1
    assert sum(result is None for result in results) == 1
    assert resolve_calls == 2
    assert transport._resolved_endpoints == endpoints and transport._resolver_claim is None


@pytest.mark.parametrize("pooled", [False, True])
def test_dns_failure_skips_to_healthy_numeric_host(monkeypatch, pooled: bool) -> None:
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (73).to_bytes(2, "little")))
        release.wait(timeout=2)

    resolve_host.cache_clear()

    def fail_hostname(*_args, **_kwargs):
        raise socket.gaierror("deterministic DNS failure")

    with Scripted7709Server([handler]) as server:
        monkeypatch.setattr(socket, "getaddrinfo", fail_hostname)
        transport = (
            PooledSocketTransport(
                ["unresolvable.invalid:7709", server.host], pool_size=1, timeout=1, heartbeat_interval=None
            )
            if pooled
            else SocketTransport(
                ["unresolvable.invalid:7709", server.host], timeout=1, heartbeat_interval=None
            )
        )
        try:
            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 73
            assert transport.connected_host == server.host
        finally:
            release.set()
            transport.close()
            resolve_host.cache_clear()


@pytest.mark.parametrize("pooled", [False, True])
def test_all_dns_failures_raise_transport_error(monkeypatch, pooled: bool) -> None:
    resolve_host.cache_clear()
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(socket.gaierror("deterministic DNS failure")),
    )
    transport = (
        PooledSocketTransport(["unresolvable.invalid:7709"], pool_size=1, timeout=1, heartbeat_interval=None)
        if pooled
        else SocketTransport(["unresolvable.invalid:7709"], timeout=1, heartbeat_interval=None)
    )

    try:
        with pytest.raises(ConnectionClosedError, match="unable to resolve"):
            transport.connect()
    finally:
        transport.close()
        resolve_host.cache_clear()


def test_pool_close_during_dns_preflight_never_publishes_broker(monkeypatch) -> None:
    entered = threading.Event()
    release = threading.Event()
    original_resolve = pool_module.resolve_hosts

    def resolve(hosts):
        entered.set()
        assert release.wait(timeout=2)
        return original_resolve(hosts)

    monkeypatch.setattr(pool_module, "resolve_hosts", resolve)
    pool = PooledSocketTransport(hosts=["127.0.0.1:9"], timeout=1, pool_size=2, heartbeat_interval=None)
    result = []
    starter = threading.Thread(target=lambda: _capture(result, pool._ensure_started))
    starter.start()
    assert entered.wait(timeout=2)
    closer = threading.Thread(target=lambda: _capture(result, pool.close))
    closer.start()
    with pool._condition:
        assert pool._condition.wait_for(lambda: pool._state is PoolState.CLOSING, timeout=2)
    release.set()
    starter.join(timeout=2)
    closer.join(timeout=2)

    assert not starter.is_alive() and not closer.is_alive()
    assert pool._broker is None and pool._push_buffer is None
    assert pool._state is PoolState.STOPPED
    assert any(isinstance(item, ConnectionClosedError) for item in result)


def _capture(results: list[object], function) -> None:
    try:
        results.append(function())
    except BaseException as exc:
        results.append(exc)
