"""Synchronous 7709 socket transport."""

from __future__ import annotations

import socket
import threading
from queue import Empty, Queue
from collections.abc import Sequence
from typing import Any

from eltdx.exceptions import ConnectionClosedError, ProtocolError, ResponseTimeoutError, TransportError
from eltdx.hosts import DEFAULT_HOSTS, unique_hosts
from eltdx.protocol.commands import build_command_frame, parse_command_response
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_HEARTBEAT
from eltdx.protocol.frame import ResponseFrame, decode_response, read_response_frame

DEFAULT_HEARTBEAT_INTERVAL = 30.0


class SocketTransport:
    """Real TCP transport for the 7709 quote protocol.

    The public API is still request/response, but reads are handled by a
    background reader so unmatched push frames do not break ordinary requests.
    """

    def __init__(
        self,
        hosts: Sequence[str] | None = None,
        *,
        timeout: float = 8.0,
        heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL,
    ) -> None:
        self._hosts = unique_hosts(list(hosts or DEFAULT_HOSTS))
        if not self._hosts:
            raise ValueError("at least one host is required")
        self._timeout = timeout
        self._heartbeat_interval = heartbeat_interval
        self._socket: socket.socket | None = None
        self._connected_host: str | None = None
        self._msg_id = 1
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending: dict[tuple[int, int], Queue[ResponseFrame | BaseException]] = {}
        self._push_queue: Queue[ResponseFrame] = Queue()
        self._stop_reader = threading.Event()
        self._stop_heartbeat = threading.Event()
        self._reader_thread: threading.Thread | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._reader_error: BaseException | None = None
        self._handshaken = False
        self.last_handshake: Any = None
        self.last_heartbeat: Any = None

    @property
    def connected_host(self) -> str | None:
        return self._connected_host

    def connect(self) -> None:
        with self._lock:
            self._ensure_socket()

    def close(self) -> None:
        with self._lock:
            self._close_socket()
            self._handshaken = False

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        request_payload = dict(payload or {})
        with self._lock:
            try:
                return self._execute_locked(command, request_payload)
            except (OSError, ConnectionClosedError, ResponseTimeoutError) as exc:
                self._close_socket()
                try:
                    return self._execute_locked(command, request_payload)
                except ResponseTimeoutError:
                    raise
                except (OSError, ConnectionClosedError) as retry_exc:
                    raise TransportError(f"7709 request failed: 0x{command:04x}") from retry_exc
            except ProtocolError:
                raise

    def request(self, command: str) -> str:
        if command == "ping":
            return "pong"
        raise ValueError(f"unsupported command: {command}")

    @property
    def pending_push_count(self) -> int:
        """Number of unmatched push frames waiting in the local queue."""

        return self._push_queue.qsize()

    def poll_push(self, timeout: float | None = 0.0, *, parse: bool = False) -> Any:
        """Return one queued push frame.

        When ``parse`` is true, the frame is parsed with an empty request
        payload. This works for single-record refresh pushes and fixed-shape
        reference table pushes; callers can keep ``parse=False`` when they need
        to inspect raw frames first.
        """

        try:
            if timeout is None:
                response = self._push_queue.get()
            elif timeout <= 0:
                response = self._push_queue.get_nowait()
            else:
                response = self._push_queue.get(timeout=timeout)
        except Empty:
            return None
        if not parse:
            return response
        return parse_command_response(response.msg_type, response, {})

    def drain_pushes(self, *, parse: bool = False) -> list[Any]:
        """Return all currently queued push frames."""

        items: list[Any] = []
        while True:
            item = self.poll_push(0, parse=parse)
            if item is None:
                return items
            items.append(item)

    def _execute_locked(self, command: int, payload: dict[str, Any]) -> Any:
        self._ensure_socket()
        if command != TYPE_HANDSHAKE and not self._handshaken:
            self.last_handshake = self._request_locked(TYPE_HANDSHAKE, {})
            self._handshaken = True

        result = self._request_locked(command, payload)
        if command == TYPE_HANDSHAKE:
            self.last_handshake = result
            self._handshaken = True
        elif command == TYPE_HEARTBEAT:
            self.last_heartbeat = result
        return result

    def _request_locked(self, command: int, payload: dict[str, Any]) -> Any:
        frame = build_command_frame(command, payload, self._next_msg_id())
        response_queue: Queue[ResponseFrame | BaseException] = Queue(maxsize=1)
        key = (frame.msg_id, frame.msg_type)
        with self._pending_lock:
            self._pending[key] = response_queue

        assert self._socket is not None
        try:
            with self._send_lock:
                self._socket.sendall(frame.to_bytes())
            try:
                response_or_error = response_queue.get(timeout=self._timeout)
            except Empty as exc:
                raise ResponseTimeoutError(f"7709 response timed out: 0x{command:04x}") from exc
            if isinstance(response_or_error, BaseException):
                raise response_or_error
            return parse_command_response(command, response_or_error, payload)
        finally:
            with self._pending_lock:
                self._pending.pop(key, None)

    def _read_response_locked(self) -> ResponseFrame:
        assert self._socket is not None
        raw = read_response_frame(self._socket)
        return decode_response(raw)

    def _ensure_socket(self) -> None:
        if self._socket is not None:
            reader_alive = self._reader_thread is not None and self._reader_thread.is_alive()
            if self._reader_error is None and reader_alive:
                return
            self._close_socket()

        last_error: OSError | None = None
        for host in self._hosts:
            address, port_text = host.rsplit(":", 1)
            try:
                sock = socket.create_connection((address, int(port_text)), timeout=self._timeout)
                sock.settimeout(self._timeout)
            except OSError as exc:
                last_error = exc
                continue
            self._socket = sock
            self._connected_host = host
            self._reader_error = None
            self._stop_reader.clear()
            self._stop_heartbeat.clear()
            self._start_reader_locked()
            self._start_heartbeat_locked()
            return
        raise ConnectionClosedError("unable to connect to any 7709 host") from last_error

    def _close_socket(self) -> None:
        self._stop_reader.set()
        self._stop_heartbeat.set()
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None
                self._connected_host = None
                self._handshaken = False
        if self._reader_thread is not None and self._reader_thread is not threading.current_thread():
            self._reader_thread.join(timeout=0.2)
        self._reader_thread = None
        if self._heartbeat_thread is not None and self._heartbeat_thread is not threading.current_thread():
            self._heartbeat_thread.join(timeout=0.2)
        self._heartbeat_thread = None
        self._fail_pending(ConnectionClosedError("socket closed"))

    def _next_msg_id(self) -> int:
        value = self._msg_id
        self._msg_id = 1 if self._msg_id >= 0xFFFFFFFF else self._msg_id + 1
        return value

    def _start_reader_locked(self) -> None:
        if self._reader_thread is not None and self._reader_thread.is_alive():
            return
        thread = threading.Thread(target=self._reader_loop, name="eltdx-7709-reader", daemon=True)
        self._reader_thread = thread
        thread.start()

    def _start_heartbeat_locked(self) -> None:
        if self._heartbeat_interval is None or self._heartbeat_interval <= 0:
            return
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return
        thread = threading.Thread(target=self._heartbeat_loop, name="eltdx-7709-heartbeat", daemon=True)
        self._heartbeat_thread = thread
        thread.start()

    def _heartbeat_loop(self) -> None:
        assert self._heartbeat_interval is not None
        while not self._stop_heartbeat.wait(self._heartbeat_interval):
            try:
                self.execute(TYPE_HEARTBEAT, {})
            except BaseException as exc:
                if not self._stop_heartbeat.is_set():
                    self._reader_error = exc
                    self._close_socket()
                return

    def _reader_loop(self) -> None:
        while not self._stop_reader.is_set():
            try:
                response = self._read_response_locked()
            except (socket.timeout, TimeoutError, ResponseTimeoutError):
                continue
            except (OSError, ConnectionClosedError) as exc:
                if not self._stop_reader.is_set():
                    self._reader_error = exc
                    error = ConnectionClosedError("7709 reader stopped")
                    error.__cause__ = exc
                    self._fail_pending(error)
                return
            except BaseException as exc:
                self._reader_error = exc
                self._fail_pending(exc)
                return
            self._route_response(response)

    def _route_response(self, response: ResponseFrame) -> None:
        key = (response.msg_id, response.msg_type)
        with self._pending_lock:
            pending = self._pending.get(key)
        if pending is not None:
            pending.put(response)
            return

        if response.msg_type == TYPE_HEARTBEAT:
            try:
                self.last_heartbeat = parse_command_response(TYPE_HEARTBEAT, response, {})
            except ProtocolError:
                self._push_queue.put(response)
            return

        self._push_queue.put(response)

    def _fail_pending(self, exc: BaseException) -> None:
        with self._pending_lock:
            queues = list(self._pending.values())
            self._pending.clear()
        for response_queue in queues:
            try:
                response_queue.put_nowait(exc)
            except Exception:
                pass
