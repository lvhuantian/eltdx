"""TDX server file command builder and parser."""

from __future__ import annotations

import operator
from typing import Any

from eltdx.exceptions import ProtocolError
from eltdx.models import FileContentChunk
from eltdx.protocol.constants import DEFAULT_FILE_CHUNK_SIZE, MAX_FILE_CHUNK_SIZE, TYPE_FILE_CONTENT
from eltdx.protocol.frame import RequestFrame, ResponseFrame
from eltdx.protocol.unit import little_u32

FILE_PATH_FIELD_SIZE = 300


def build_file_content_frame(payload: dict[str, Any], msg_id: int) -> RequestFrame:
    path = _normalize_path(payload.get("path"))
    offset = _u32(payload.get("offset", 0), "offset")
    size = _u32(payload.get("size", DEFAULT_FILE_CHUNK_SIZE), "size")
    if size == 0:
        raise ProtocolError("file content size must be > 0")
    if size > MAX_FILE_CHUNK_SIZE:
        raise ProtocolError(f"file content size must be <= {MAX_FILE_CHUNK_SIZE}")

    path_raw = path.encode("ascii")
    if len(path_raw) > FILE_PATH_FIELD_SIZE:
        raise ProtocolError("file content path exceeds 300 ASCII bytes")
    data = (
        offset.to_bytes(4, "little", signed=False)
        + size.to_bytes(4, "little", signed=False)
        + path_raw.ljust(FILE_PATH_FIELD_SIZE, b"\x00")
    )
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_FILE_CONTENT, data=data)


def parse_file_content_payload(
    response: ResponseFrame,
    request_payload: dict[str, Any] | None = None,
) -> FileContentChunk:
    request_payload = request_payload or {}
    payload = response.data
    if len(payload) < 4:
        raise ProtocolError("invalid file content payload")

    chunk_len = little_u32(payload[:4])
    expected_length = 4 + chunk_len
    if len(payload) < expected_length:
        raise ProtocolError(f"invalid file content payload length: expected {expected_length}, got {len(payload)}")

    request_size = _u32(request_payload.get("size", DEFAULT_FILE_CHUNK_SIZE), "size")
    if request_size == 0:
        raise ProtocolError("file content size must be > 0")
    if request_size > MAX_FILE_CHUNK_SIZE:
        raise ProtocolError(f"file content size must be <= {MAX_FILE_CHUNK_SIZE}")
    if chunk_len > request_size:
        raise ProtocolError(f"file content chunk exceeds requested size: {chunk_len} > {request_size}")

    return FileContentChunk(
        path=_normalize_path(request_payload.get("path")),
        offset=_u32(request_payload.get("offset", 0), "offset"),
        request_size=request_size,
        chunk_len=chunk_len,
        content=payload[4:expected_length],
        raw_payload=payload,
    )


def _normalize_path(value: Any) -> str:
    if not isinstance(value, str):
        raise ProtocolError("file content path must be a string")
    path = value.strip().replace("\\", "/")
    if not path:
        raise ProtocolError("file content path is required")
    if "\x00" in path:
        raise ProtocolError("file content path must not contain NUL")
    try:
        path.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ProtocolError("file content path must be ASCII") from exc
    return path


def _u32(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ProtocolError(f"file content {name} must be an integer")
    try:
        number = operator.index(value)
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"file content {name} must be an integer") from exc
    if not 0 <= number <= 0xFFFFFFFF:
        raise ProtocolError(f"file content {name} out of uint32 range")
    return number
