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
from eltdx.hosts import resolve_hosts
from eltdx.protocol.constants import TYPE_SECURITY_COUNT
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


def test_close_during_dns_preflight_discards_result_before_actor_start(monkeypatch) -> None:
    entered = threading.Event()
    release = threading.Event()
    original_resolve = socket_module.resolve_hosts

    def resolve(hosts):
        entered.set()
        assert release.wait(timeout=2)
        return original_resolve(hosts)

    monkeypatch.setattr(socket_module, "resolve_hosts", resolve)
    transport = SocketTransport(hosts=["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    result = []
    starter = threading.Thread(target=lambda: _capture(result, transport._ensure_runtime))
    starter.start()
    assert entered.wait(timeout=2)
    closer = threading.Thread(target=lambda: _capture(result, transport.close))
    closer.start()
    with transport._lifecycle:
        assert transport._lifecycle.wait_for(lambda: transport._closing, timeout=2)
    release.set()
    starter.join(timeout=2)
    closer.join(timeout=2)

    assert not starter.is_alive() and not closer.is_alive()
    assert transport._runtime is None
    assert not any(thread.name.startswith("eltdx-7709-actor-") and thread.is_alive() for thread in threading.enumerate())
    assert any(isinstance(item, ConnectionClosedError) for item in result)


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
