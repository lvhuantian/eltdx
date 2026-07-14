from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx.exceptions import ConnectionClosedError, PoolBusyError
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_SECURITY_COUNT
from eltdx.transport import PooledSocketTransport
from eltdx.transport.pool import HeartbeatAdmissionGuard, LeaseBroker
from eltdx.transport import socket as socket_module


def test_pooled_socket_transport_rotates_hosts_per_connection() -> None:
    transport = PooledSocketTransport(hosts=["127.0.0.1:1", "127.0.0.1:2"], timeout=0.1, pool_size=3, heartbeat_interval=None)

    assert transport.hosts == ("127.0.0.1:1", "127.0.0.1:2")
    assert transport.pool_size == 3
    assert transport.heartbeat_interval is None
    assert [item._hosts for item in transport._transports] == [
        ["127.0.0.1:1", "127.0.0.1:2"],
        ["127.0.0.1:2", "127.0.0.1:1"],
        ["127.0.0.1:1", "127.0.0.1:2"],
    ]


def test_lease_broker_assigns_waiters_fifo_and_releases_exactly_once() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=4)
    initial = broker.acquire(time.monotonic() + 2)
    order = []

    def acquire(index: int) -> None:
        lease = broker.acquire(time.monotonic() + 2)
        order.append(index)
        assert broker.release(lease)
        assert not broker.release(lease)

    threads = []
    for index in range(3):
        thread = threading.Thread(target=acquire, args=(index,))
        thread.start()
        threads.append(thread)
        assert broker.wait_for_waiters(index + 1)
    assert broker.release(initial)
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()

    assert order == [0, 1, 2]
    snapshot = broker.snapshot()
    assert snapshot.idle_slots == 1
    assert snapshot.waiter_count == 0
    assert snapshot.active_leases == 0


def test_pooled_heartbeat_guard_defers_for_business_pressure() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=4)
    guard = HeartbeatAdmissionGuard(broker)

    assert guard()
    lease = broker.acquire(time.monotonic() + 1)
    assert not guard()
    assert broker.release(lease)
    assert guard()

    pinned = broker.acquire(time.monotonic() + 1, pinned=True)
    assert guard()
    broker.reserve_pin_waiter()
    assert not guard()
    broker.release_pin_waiter()
    assert guard()
    assert broker.release(pinned)


def test_pool_uses_first_idle_slot_instead_of_queueing_behind_slow_slot() -> None:
    slow_received = threading.Event()
    release_slow = threading.Event()
    release_server = threading.Event()

    def slow(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        slow_received.set()
        assert release_slow.wait(timeout=2)
        conn.sendall(response_bytes(msg_id, msg_type, (100).to_bytes(2, "little")))
        release_server.wait(timeout=2)

    def fast(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        for value in (200, 201):
            msg_id, msg_type, _ = read_request(conn)
            conn.sendall(response_bytes(msg_id, msg_type, value.to_bytes(2, "little")))
        release_server.wait(timeout=2)

    with Scripted7709Server([slow, fast]) as server:
        pool = PooledSocketTransport(hosts=[server.host], timeout=2, pool_size=2, heartbeat_interval=None)
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                slow_result = executor.submit(pool.execute, TYPE_SECURITY_COUNT, {"market": "sz"})
                assert slow_received.wait(timeout=2)
                assert pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 200
                assert pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 201
                release_slow.set()
                assert slow_result.result() == 100
        finally:
            release_slow.set()
            release_server.set()
            pool.close()


def test_pin_holds_one_slot_and_ordinary_work_uses_other_slot() -> None:
    release_server = threading.Event()
    calls = [[], []]

    def handler(index: int):
        def serve(conn: socket.socket) -> None:
            msg_id, msg_type, _ = read_request(conn)
            conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
            expected = 2 if index == 0 else 1
            for _ in range(expected):
                msg_id, msg_type, _ = read_request(conn)
                calls[index].append(msg_id)
                conn.sendall(response_bytes(msg_id, msg_type, (300 + index).to_bytes(2, "little")))
            release_server.wait(timeout=2)

        return serve

    with Scripted7709Server([handler(0), handler(1)]) as server:
        pool = PooledSocketTransport(hosts=[server.host], timeout=2, pool_size=2, heartbeat_interval=None)
        try:
            with pool.pin() as pinned:
                assert pinned.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 300
                assert pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 301
                assert pinned.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 300
                assert pinned.connected_host == server.host
        finally:
            release_server.set()
            pool.close()

    assert [len(items) for items in calls] == [2, 1]


def test_bounded_admission_rejects_overflow_and_close_wakes_waiter() -> None:
    broker = LeaseBroker(2, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 2)
    entered = threading.Event()
    result = []

    def wait_for_slot() -> None:
        entered.set()
        try:
            broker.acquire(time.monotonic() + 2)
        except BaseException as exc:
            result.append(exc)

    thread = threading.Thread(target=wait_for_slot)
    thread.start()
    assert entered.wait(timeout=2)
    assert broker.wait_for_waiters(1)
    with pytest.raises(PoolBusyError, match="queue is full"):
        broker.acquire(time.monotonic() + 2)
    broker.close()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
    assert not broker.release(lease)


def test_timed_out_admission_waiter_is_atomically_removed() -> None:
    broker = LeaseBroker(3, pool_size=1, max_pending_requests=1)
    lease = broker.acquire(time.monotonic() + 1)

    with pytest.raises(Exception, match="timed out during queue"):
        broker.acquire(time.monotonic() + 0.02)

    assert broker.snapshot().waiter_count == 0
    assert broker.release(lease)


def test_partial_pool_connect_failure_stops_and_closes_every_slot(monkeypatch) -> None:
    pool = PooledSocketTransport(hosts=["127.0.0.1:9"], timeout=0.1, pool_size=3, heartbeat_interval=None)
    stopped = []
    closed = []

    for index, transport in enumerate(pool._transports):
        if index == 1:
            monkeypatch.setattr(transport, "connect", lambda: (_ for _ in ()).throw(ConnectionClosedError("slot failed")))
        else:
            monkeypatch.setattr(transport, "connect", lambda: None)
        monkeypatch.setattr(transport, "_request_stop", lambda index=index: stopped.append(index))
        monkeypatch.setattr(transport, "_close_with_timeout", lambda timeout, index=index: closed.append(index))

    with pytest.raises(ConnectionClosedError, match="slot failed"):
        pool.connect()

    assert sorted(stopped) == [0, 1, 2]
    assert sorted(closed) == [0, 1, 2]


def test_pinned_multithread_waiters_advance_at_wire_terminal_before_parser(monkeypatch) -> None:
    first_received = threading.Event()
    release_first_response = threading.Event()
    parser_entered = threading.Event()
    release_parser = threading.Event()
    second_received = threading.Event()
    release_server = threading.Event()
    original_parse = socket_module.parse_command_response

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        first_received.set()
        assert release_first_response.wait(timeout=2)
        conn.sendall(response_bytes(msg_id, msg_type, (111).to_bytes(2, "little")))
        msg_id, msg_type, _ = read_request(conn)
        second_received.set()
        conn.sendall(response_bytes(msg_id, msg_type, (222).to_bytes(2, "little")))
        release_server.wait(timeout=2)

    def parse(command, response, payload=None):
        if response.data == (111).to_bytes(2, "little"):
            parser_entered.set()
            assert release_parser.wait(timeout=2)
        return original_parse(command, response, payload)

    monkeypatch.setattr(socket_module, "parse_command_response", parse)
    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport(hosts=[server.host], timeout=2, pool_size=1, heartbeat_interval=None)
        try:
            with pool.pin() as pinned, ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(pinned.execute, TYPE_SECURITY_COUNT, {"market": "sz"})
                assert first_received.wait(timeout=2)
                second = executor.submit(pinned.execute, TYPE_SECURITY_COUNT, {"market": "sz"})
                release_first_response.set()
                assert parser_entered.wait(timeout=2)
                assert second_received.wait(timeout=2)
                release_parser.set()
                assert (first.result(), second.result()) == (111, 222)
        finally:
            release_first_response.set()
            release_parser.set()
            release_server.set()
            pool.close()


def test_normal_lease_returns_to_fifo_before_caller_parser_runs(monkeypatch) -> None:
    parser_entered = threading.Event()
    release_parser = threading.Event()
    second_received = threading.Event()
    release_server = threading.Event()
    original_parse = socket_module.parse_command_response

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (11).to_bytes(2, "little")))
        msg_id, msg_type, _ = read_request(conn)
        second_received.set()
        conn.sendall(response_bytes(msg_id, msg_type, (22).to_bytes(2, "little")))
        release_server.wait(timeout=2)

    def parse(command, response, payload=None):
        if response.data == (11).to_bytes(2, "little"):
            parser_entered.set()
            assert release_parser.wait(timeout=2)
        return original_parse(command, response, payload)

    monkeypatch.setattr(socket_module, "parse_command_response", parse)
    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport(hosts=[server.host], timeout=2, pool_size=1, heartbeat_interval=None)
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(pool.execute, TYPE_SECURITY_COUNT, {"market": "sz"})
                assert parser_entered.wait(timeout=2)
                second = executor.submit(pool.execute, TYPE_SECURITY_COUNT, {"market": "sz"})
                assert second_received.wait(timeout=2)
                release_parser.set()
                assert (first.result(), second.result()) == (11, 22)
        finally:
            release_parser.set()
            release_server.set()
            pool.close()


def test_pool_close_does_not_wait_for_lost_pin_and_invalidates_old_proxy() -> None:
    pool = PooledSocketTransport(hosts=["127.0.0.1:9"], timeout=1, pool_size=2, heartbeat_interval=None)
    context = pool.pin()
    pinned = context.__enter__()
    assert pool._push_buffer is not None
    assert all(slot._shared_push_buffer is pool._push_buffer for slot in pool._transports)

    started = time.perf_counter()
    pool.close()

    assert time.perf_counter() - started < 0.1
    with pytest.raises(ConnectionClosedError, match="lease is no longer valid"):
        pinned.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
    context.__exit__(None, None, None)


def test_pinned_local_waiters_share_global_pending_capacity() -> None:
    first_received = threading.Event()
    release_first = threading.Event()
    release_server = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        first_received.set()
        assert release_first.wait(timeout=2)
        conn.sendall(response_bytes(msg_id, msg_type, (1).to_bytes(2, "little")))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (2).to_bytes(2, "little")))
        release_server.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport(
            hosts=[server.host], timeout=2, pool_size=1, heartbeat_interval=None, max_pending_requests=1
        )
        try:
            with pool.pin() as pinned, ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(pinned.execute, TYPE_SECURITY_COUNT, {"market": "sz"})
                assert first_received.wait(timeout=2)
                second = executor.submit(pinned.execute, TYPE_SECURITY_COUNT, {"market": "sz"})
                assert pool._broker is not None and pool._broker.wait_for_pin_waiters(1)
                with pytest.raises(PoolBusyError, match="queue is full"):
                    pinned.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
                release_first.set()
                assert (first.result(), second.result()) == (1, 2)
        finally:
            release_first.set()
            release_server.set()
            pool.close()
