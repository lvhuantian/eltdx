"""TDX server file models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FileContentChunk:
    path: str
    offset: int
    request_size: int
    chunk_len: int
    content: bytes
    raw_payload: bytes = b""

    @property
    def is_last(self) -> bool:
        return self.chunk_len < self.request_size
