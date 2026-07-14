"""7709 TCP frame encoding and decoding."""

from __future__ import annotations

import socket
import struct
import zlib
from dataclasses import dataclass

from eltdx.exceptions import ConnectionClosedError, ProtocolError

from .constants import CONTROL_DEFAULT, PREFIX, PREFIX_RESP


RESPONSE_HEADER_SIZE = 16
MAX_RESPONSE_PAYLOAD_SIZE = 0xFFFF
MAX_RESPONSE_BUFFER_SIZE = RESPONSE_HEADER_SIZE + MAX_RESPONSE_PAYLOAD_SIZE
MAX_RESPONSE_RESYNC_BYTES = 0x10000


@dataclass(frozen=True, slots=True)
class RequestFrame:
    msg_id: int
    msg_type: int
    data: bytes = b""
    control: int = CONTROL_DEFAULT

    def to_bytes(self) -> bytes:
        length = len(self.data) + 2
        return struct.pack("<BIBHHH", PREFIX, self.msg_id, self.control, length, length, self.msg_type) + self.data


@dataclass(frozen=True, slots=True)
class ResponseFrame:
    control: int
    msg_id: int
    msg_type: int
    zip_length: int
    length: int
    data: bytes
    raw: bytes
    response_header_reserved: int = 0


def read_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        piece = sock.recv(size - len(chunks))
        if not piece:
            raise ConnectionClosedError("socket closed by remote peer")
        chunks.extend(piece)
    return bytes(chunks)


def read_response_frame(sock: socket.socket) -> bytes:
    window = bytearray(read_exact(sock, 4))
    while bytes(window) != PREFIX_RESP:
        window = window[1:] + read_exact(sock, 1)

    header = read_exact(sock, 12)
    zip_length = int.from_bytes(header[8:10], "little", signed=False)
    payload = read_exact(sock, zip_length)
    return bytes(window) + header + payload


def decode_response(raw: bytes, *, max_payload_size: int = MAX_RESPONSE_PAYLOAD_SIZE) -> ResponseFrame:
    if len(raw) < RESPONSE_HEADER_SIZE:
        raise ProtocolError(f"invalid response length: {len(raw)}")
    if raw[:4] != PREFIX_RESP:
        raise ProtocolError(f"invalid response prefix: {raw[:4].hex()}")

    control = raw[4]
    msg_id = int.from_bytes(raw[5:9], "little", signed=False)
    reserved = raw[9]
    msg_type = int.from_bytes(raw[10:12], "little", signed=False)
    zip_length = int.from_bytes(raw[12:14], "little", signed=False)
    length = int.from_bytes(raw[14:16], "little", signed=False)
    payload = raw[RESPONSE_HEADER_SIZE:]

    if zip_length > max_payload_size:
        raise ProtocolError(f"compressed payload exceeds limit: {zip_length} > {max_payload_size}")
    if length > max_payload_size:
        raise ProtocolError(f"decoded payload exceeds limit: {length} > {max_payload_size}")

    if len(payload) != zip_length:
        raise ProtocolError(f"zip length mismatch: expected {zip_length}, got {len(payload)}")

    data = _decode_payload(payload, zip_length=zip_length, length=length)
    if len(data) != length:
        raise ProtocolError(f"decoded length mismatch: expected {length}, got {len(data)}")

    return ResponseFrame(
        control=control,
        msg_id=msg_id,
        msg_type=msg_type,
        zip_length=zip_length,
        length=length,
        data=data,
        raw=raw,
        response_header_reserved=reserved,
    )


class ResponseFrameDecoder:
    """Incrementally decode bounded 7709 response frames for one TCP generation."""

    def __init__(
        self,
        *,
        max_payload_size: int = MAX_RESPONSE_PAYLOAD_SIZE,
        max_buffer_size: int = MAX_RESPONSE_BUFFER_SIZE,
        max_resync_bytes: int = MAX_RESPONSE_RESYNC_BYTES,
    ) -> None:
        if max_payload_size < 0 or max_payload_size > MAX_RESPONSE_PAYLOAD_SIZE:
            raise ValueError(f"max_payload_size must be between 0 and {MAX_RESPONSE_PAYLOAD_SIZE}")
        if max_buffer_size < RESPONSE_HEADER_SIZE + max_payload_size:
            raise ValueError("max_buffer_size cannot hold the largest configured frame")
        if max_resync_bytes < 0:
            raise ValueError("max_resync_bytes must be >= 0")
        self._max_payload_size = max_payload_size
        self._max_buffer_size = max_buffer_size
        self._max_resync_bytes = max_resync_bytes
        self._buffer = bytearray()
        self._resync_discarded = 0
        self._max_buffer_observed = 0

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)

    @property
    def resync_discarded(self) -> int:
        return self._resync_discarded

    @property
    def max_buffer_observed(self) -> int:
        return self._max_buffer_observed

    def feed(self, data: bytes | bytearray | memoryview) -> list[ResponseFrame]:
        view = memoryview(data)
        frames: list[ResponseFrame] = []
        offset = 0
        while offset < len(view):
            capacity = self._max_buffer_size - len(self._buffer)
            if capacity <= 0:
                frames.extend(self._drain())
                capacity = self._max_buffer_size - len(self._buffer)
                if capacity <= 0:
                    raise ProtocolError(f"response buffer exceeds limit: {self._max_buffer_size}")
            chunk_size = min(capacity, len(view) - offset)
            self._buffer.extend(view[offset : offset + chunk_size])
            offset += chunk_size
            self._max_buffer_observed = max(self._max_buffer_observed, len(self._buffer))
            frames.extend(self._drain())
        return frames

    def finish(self) -> list[ResponseFrame]:
        frames = self._drain()
        if self._buffer:
            raise ProtocolError(f"truncated response frame at EOF: {len(self._buffer)} buffered bytes")
        return frames

    def _drain(self) -> list[ResponseFrame]:
        frames: list[ResponseFrame] = []
        while self._buffer:
            prefix_index = self._buffer.find(PREFIX_RESP)
            if prefix_index < 0:
                keep = _prefix_suffix_length(self._buffer)
                self._discard_resync(len(self._buffer) - keep)
                if keep:
                    del self._buffer[:-keep]
                else:
                    self._buffer.clear()
                break
            if prefix_index:
                self._discard_resync(prefix_index)
                del self._buffer[:prefix_index]
            if len(self._buffer) < RESPONSE_HEADER_SIZE:
                break

            zip_length = int.from_bytes(self._buffer[12:14], "little", signed=False)
            length = int.from_bytes(self._buffer[14:16], "little", signed=False)
            if zip_length > self._max_payload_size:
                raise ProtocolError(f"compressed payload exceeds limit: {zip_length} > {self._max_payload_size}")
            if length > self._max_payload_size:
                raise ProtocolError(f"decoded payload exceeds limit: {length} > {self._max_payload_size}")
            frame_size = RESPONSE_HEADER_SIZE + zip_length
            if frame_size > self._max_buffer_size:
                raise ProtocolError(f"response frame exceeds buffer limit: {frame_size} > {self._max_buffer_size}")
            if len(self._buffer) < frame_size:
                break
            raw = bytes(self._buffer[:frame_size])
            del self._buffer[:frame_size]
            frames.append(decode_response(raw, max_payload_size=self._max_payload_size))
        return frames

    def _discard_resync(self, count: int) -> None:
        self._resync_discarded += count
        if self._resync_discarded > self._max_resync_bytes:
            raise ProtocolError(
                f"response resync exceeds limit: {self._resync_discarded} > {self._max_resync_bytes}"
            )


def _prefix_suffix_length(data: bytearray) -> int:
    maximum = min(len(data), len(PREFIX_RESP) - 1)
    for size in range(maximum, 0, -1):
        if data[-size:] == PREFIX_RESP[:size]:
            return size
    return 0


def _decode_payload(payload: bytes, *, zip_length: int, length: int) -> bytes:
    if zip_length == length:
        return payload
    try:
        decoder = zlib.decompressobj()
        data = decoder.decompress(payload, length + 1)
    except zlib.error as exc:
        raise ProtocolError(f"invalid compressed response payload: {exc}") from exc
    if len(data) > length:
        raise ProtocolError(f"decoded payload exceeds declared length: {len(data)} > {length}")
    if decoder.unconsumed_tail:
        raise ProtocolError("compressed response exceeds declared length")
    if not decoder.eof:
        raise ProtocolError("compressed response ended before zlib stream EOF")
    if decoder.unused_data:
        raise ProtocolError("compressed response contains trailing data")
    return data
