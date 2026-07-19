from __future__ import annotations

import socket
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import pytest

from eltdx.exceptions import PushOverflowError
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_HEARTBEAT, TYPE_REFRESH_STREAM, TYPE_SECURITY_COUNT
from eltdx.transport import SocketTransport
from eltdx.transport import socket as socket_module


Request = tuple[int, int, bytes]
ConnectionHandler = Callable[[socket.socket], None]


class Scripted7709Server:
    def __init__(self, handlers: list[ConnectionHandler]) -> None:
        self._handlers = handlers
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._thread: threading.Thread | None = None
        self.errors: list[BaseException] = []
        self.host = ""

    def __enter__(self) -> Scripted7709Server:
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(("127.0.0.1", 0))
        self._socket.listen()
        self._socket.settimeout(2)
        address, port = self._socket.getsockname()
        self.host = f"{address}:{port}"
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._socket.close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if exc_type is None and self.errors:
            raise AssertionError(f"fake server failed: {self.errors!r}")

    def _serve(self) -> None:
        try:
            for handler in self._handlers:
                conn, _ = self._socket.accept()
                conn.settimeout(2)
                with conn:
                    handler(conn)
        except OSError:
            return
        except BaseException as exc:
            self.errors.append(exc)


def test_socket_transport_handles_push_before_matching_response() -> None:
    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = _read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(_response(msg_id, TYPE_HANDSHAKE, _handshake_payload()))

        msg_id, msg_type, _ = _read_request(conn)
        assert msg_type == TYPE_SECURITY_COUNT
        conn.sendall(_response(0x290000, TYPE_REFRESH_STREAM, bytes.fromhex("9393")))
        conn.sendall(_response(msg_id, TYPE_SECURITY_COUNT, (123).to_bytes(2, "little")))

    with Scripted7709Server([handler]) as server:
        transport = SocketTransport(hosts=[server.host], timeout=1)
        try:
            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 123
            assert transport.pending_push_count == 1
            assert transport.poll_push(parse=True).count == 0
        finally:
            transport.close()


def test_socket_transport_reconnects_after_reader_disconnect() -> None:
    release_second_connection = threading.Event()

    def first_connection(conn: socket.socket) -> None:
        msg_id, msg_type, _ = _read_request(conn)
        assert msg_type == TYPE_HANDSHAKE

    def second_connection(conn: socket.socket) -> None:
        msg_id, msg_type, _ = _read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(_response(msg_id, TYPE_HANDSHAKE, _handshake_payload()))

        msg_id, msg_type, _ = _read_request(conn)
        assert msg_type == TYPE_SECURITY_COUNT
        conn.sendall(_response(msg_id, TYPE_SECURITY_COUNT, (321).to_bytes(2, "little")))
        release_second_connection.wait()

    with Scripted7709Server([first_connection, second_connection]) as server:
        transport = SocketTransport(hosts=[server.host], timeout=1)
        try:
            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 321
            assert transport.connected_host == server.host
        finally:
            release_second_connection.set()
            transport.close()


def test_socket_transport_serializes_concurrent_requests_without_crossing_responses() -> None:
    request_count = 5

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = _read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(_response(msg_id, TYPE_HANDSHAKE, _handshake_payload()))

        for index in range(request_count):
            msg_id, msg_type, _ = _read_request(conn)
            assert msg_type == TYPE_SECURITY_COUNT
            conn.sendall(_response(msg_id, TYPE_SECURITY_COUNT, (100 + index).to_bytes(2, "little")))

    with Scripted7709Server([handler]) as server:
        transport = SocketTransport(hosts=[server.host], timeout=1)
        try:
            with ThreadPoolExecutor(max_workers=request_count) as pool:
                results = list(pool.map(lambda _: transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}), range(request_count)))
        finally:
            transport.close()

    assert sorted(results) == [100, 101, 102, 103, 104]


def test_socket_transport_background_heartbeat_uses_same_connection() -> None:
    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = _read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(_response(msg_id, TYPE_HANDSHAKE, _handshake_payload()))

        msg_id, msg_type, _ = _read_request(conn)
        assert msg_type == TYPE_HEARTBEAT
        conn.sendall(_response(msg_id, TYPE_HEARTBEAT, bytes.fromhex("0000000000008f173501")))

    with Scripted7709Server([handler]) as server:
        transport = SocketTransport(hosts=[server.host], timeout=1, heartbeat_interval=0.02)
        try:
            transport.connect()
            for _ in range(100):
                if transport.last_heartbeat is not None:
                    break
                threading.Event().wait(0.01)
        finally:
            transport.close()

    assert transport.last_heartbeat is not None


def test_business_parser_runs_in_caller_after_wire_slot_is_released(monkeypatch) -> None:
    parser_entered = threading.Event()
    release_parser = threading.Event()
    second_request_seen = threading.Event()
    release_server = threading.Event()
    original_parse = socket_module.parse_command_response

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = _read_request(conn)
        conn.sendall(_response(msg_id, msg_type, _handshake_payload()))
        msg_id, msg_type, _ = _read_request(conn)
        conn.sendall(_response(msg_id, msg_type, (111).to_bytes(2, "little")))
        msg_id, msg_type, _ = _read_request(conn)
        second_request_seen.set()
        conn.sendall(_response(msg_id, msg_type, (222).to_bytes(2, "little")))
        release_server.wait(timeout=2)

    def parse(command, response, payload=None):
        if response.data == (111).to_bytes(2, "little"):
            parser_entered.set()
            assert release_parser.wait(timeout=2)
        return original_parse(command, response, payload)

    monkeypatch.setattr(socket_module, "parse_command_response", parse)
    with Scripted7709Server([handler]) as server:
        transport = SocketTransport(hosts=[server.host], timeout=2, heartbeat_interval=None)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(transport.execute, TYPE_SECURITY_COUNT, {"market": "sz"})
                assert parser_entered.wait(timeout=2)
                second = pool.submit(transport.execute, TYPE_SECURITY_COUNT, {"market": "sz"})
                assert second_request_seen.wait(timeout=2)
                release_parser.set()
                assert (first.result(), second.result()) == (111, 222)
        finally:
            release_parser.set()
            release_server.set()
            transport.close()


def test_push_flood_is_bounded_and_does_not_starve_matching_response() -> None:
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = _read_request(conn)
        conn.sendall(_response(msg_id, msg_type, _handshake_payload()))
        msg_id, msg_type, _ = _read_request(conn)
        for index in range(200):
            conn.sendall(_response(0x290000 + index, TYPE_REFRESH_STREAM, bytes.fromhex("9393")))
        conn.sendall(_response(msg_id, msg_type, (456).to_bytes(2, "little")))
        release.wait()

    with Scripted7709Server([handler]) as server:
        transport = SocketTransport(
            hosts=[server.host],
            timeout=2,
            heartbeat_interval=None,
            push_queue_size=10,
            push_queue_bytes=1024,
        )
        try:
            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 456
            assert transport.pending_push_count <= 10
            assert transport._push_buffer is not None
            snapshot = transport._push_buffer.snapshot()
            assert snapshot.max_frames_observed <= 10
            assert snapshot.max_bytes_observed <= 1024
            with pytest.raises(PushOverflowError, match="push gap"):
                transport.poll_push()
        finally:
            release.set()
            transport.close()


def _read_request(conn: socket.socket) -> Request:
    header = _read_exact(conn, 12)
    assert header[0] == 0x0C
    msg_id = int.from_bytes(header[1:5], "little")
    length = int.from_bytes(header[6:8], "little")
    msg_type = int.from_bytes(header[10:12], "little")
    payload = _read_exact(conn, length - 2)
    return msg_id, msg_type, payload


def _response(msg_id: int, msg_type: int, payload: bytes) -> bytes:
    return (
        b"\xb1\xcb\x74\x00"
        + b"\x00"
        + msg_id.to_bytes(4, "little")
        + b"\x00"
        + msg_type.to_bytes(2, "little")
        + len(payload).to_bytes(2, "little")
        + len(payload).to_bytes(2, "little")
        + payload
    )


def _handshake_payload() -> bytes:
    payload = bytearray(189)
    payload[1:3] = (2026).to_bytes(2, "little")
    payload[3] = 27
    payload[4] = 5
    payload[5] = 30
    payload[6] = 10
    payload[8] = 0
    payload[42:46] = (20260527).to_bytes(4, "little")
    payload[50:54] = (20260527).to_bytes(4, "little")
    payload[68:152] = b"fake-7709".ljust(84, b"\x00")
    payload[160:189] = b"fake-product".ljust(29, b"\x00")
    return bytes(payload)


def _read_exact(conn: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        piece = conn.recv(size - len(chunks))
        if not piece:
            raise EOFError("connection closed")
        chunks.extend(piece)
    return bytes(chunks)
