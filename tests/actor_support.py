"""Deterministic real-socket support for 7709 transport tests."""

from __future__ import annotations

import socket
import threading
from collections.abc import Callable, Sequence


Request = tuple[int, int, bytes]
ConnectionHandler = Callable[[socket.socket], None]


class Scripted7709Server:
    """Dispatch accepted loopback connections to a fixed handler sequence."""

    def __init__(self, handlers: Sequence[ConnectionHandler]) -> None:
        self._handlers = tuple(handlers)
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._thread: threading.Thread | None = None
        self._workers: list[threading.Thread] = []
        self._condition = threading.Condition()
        self._accepted = 0
        self._finished = 0
        self._closing = False
        self.errors: list[BaseException] = []
        self.host = ""

    def __enter__(self) -> Scripted7709Server:
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        address, port = self._listener.getsockname()
        self.host = f"{address}:{port}"
        self._thread = threading.Thread(target=self._serve, name="eltdx-scripted-server", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._condition:
            self._closing = True
        self._listener.close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        for worker in self._workers:
            worker.join(timeout=2)
        alive = [worker.name for worker in self._workers if worker.is_alive()]
        if exc_type is None and alive:
            raise AssertionError(f"scripted server workers did not stop: {alive!r}")
        if exc_type is None and self.errors:
            raise AssertionError(f"scripted server failed: {self.errors!r}")

    @property
    def accepted_count(self) -> int:
        with self._condition:
            return self._accepted

    def wait_for_connections(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(lambda: self._accepted >= count, timeout=timeout)

    def wait_for_handlers(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(lambda: self._finished >= count, timeout=timeout)

    def _serve(self) -> None:
        try:
            for index, handler in enumerate(self._handlers):
                conn, _ = self._listener.accept()
                conn.settimeout(2)
                with self._condition:
                    self._accepted += 1
                    self._condition.notify_all()
                worker = threading.Thread(
                    target=self._run_handler,
                    args=(conn, handler),
                    name=f"eltdx-scripted-handler-{index}",
                    daemon=True,
                )
                self._workers.append(worker)
                worker.start()
        except OSError as exc:
            with self._condition:
                closing = self._closing
            if not closing:
                self.errors.append(exc)
        except BaseException as exc:
            self.errors.append(exc)

    def _run_handler(self, conn: socket.socket, handler: ConnectionHandler) -> None:
        try:
            with conn:
                handler(conn)
        except BaseException as exc:
            self.errors.append(exc)
        finally:
            with self._condition:
                self._finished += 1
                self._condition.notify_all()


def read_exact(conn: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        piece = conn.recv(size - len(chunks))
        if not piece:
            raise EOFError("connection closed")
        chunks.extend(piece)
    return bytes(chunks)


def read_request(conn: socket.socket) -> Request:
    header = read_exact(conn, 12)
    if header[0] != 0x0C:
        raise AssertionError(f"invalid request prefix: {header[0]:#x}")
    msg_id = int.from_bytes(header[1:5], "little")
    length = int.from_bytes(header[6:8], "little")
    msg_type = int.from_bytes(header[10:12], "little")
    return msg_id, msg_type, read_exact(conn, length - 2)


def response_bytes(msg_id: int, msg_type: int, payload: bytes) -> bytes:
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


def handshake_payload() -> bytes:
    payload = bytearray(189)
    payload[1:3] = (2026).to_bytes(2, "little")
    payload[3:9] = bytes((27, 5, 30, 10, 0, 0))
    payload[42:46] = (20260527).to_bytes(4, "little")
    payload[50:54] = (20260527).to_bytes(4, "little")
    payload[68:152] = b"actor-test-7709".ljust(84, b"\x00")
    payload[160:189] = b"actor-test-product".ljust(29, b"\x00")
    return bytes(payload)
