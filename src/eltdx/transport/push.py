"""Bounded epoch-scoped push frame buffering."""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass

from eltdx.exceptions import PushOverflowError, TransportCloseTimeoutError
from eltdx.protocol.frame import ResponseFrame


@dataclass(frozen=True, slots=True)
class PushFrame:
    runtime_epoch: int
    tcp_generation: int
    connected_host: str
    response: ResponseFrame

    @property
    def wire_size(self) -> int:
        return len(self.response.raw) if self.response.raw else 16 + self.response.zip_length


@dataclass(frozen=True, slots=True)
class PushBufferSnapshot:
    owner_epoch: int
    frame_count: int
    byte_count: int
    max_frames_observed: int
    max_bytes_observed: int
    dropped_total: int
    gap_pending: bool
    closed: bool


class PushBuffer:
    def __init__(self, owner_epoch: int, *, max_frames: int = 1024, max_bytes: int = 8 * 1024 * 1024) -> None:
        if max_frames <= 0:
            raise ValueError("max_frames must be > 0")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be > 0")
        self.owner_epoch = owner_epoch
        self.max_frames = int(max_frames)
        self.max_bytes = int(max_bytes)
        self._condition = threading.Condition()
        self._frames: deque[PushFrame] = deque()
        self._bytes = 0
        self._max_frames_observed = 0
        self._max_bytes_observed = 0
        self._dropped_total = 0
        self._gap_pending = False
        self._closed = False
        self._error: BaseException | None = None

    @property
    def pending_count(self) -> int:
        with self._condition:
            return len(self._frames)

    def offer_nowait(self, frame: PushFrame) -> bool:
        if frame.runtime_epoch != self.owner_epoch:
            return False
        size = frame.wire_size
        with self._condition:
            if self._closed:
                return False
            dropped = 0
            while self._frames and (len(self._frames) >= self.max_frames or self._bytes + size > self.max_bytes):
                old = self._frames.popleft()
                self._bytes -= old.wire_size
                dropped += 1
            if size > self.max_bytes:
                dropped += 1
                accepted = False
            else:
                self._frames.append(frame)
                self._bytes += size
                accepted = True
            if dropped:
                self._dropped_total += dropped
                self._gap_pending = True
            self._max_frames_observed = max(self._max_frames_observed, len(self._frames))
            self._max_bytes_observed = max(self._max_bytes_observed, self._bytes)
            self._condition.notify()
            return accepted

    def poll(self, timeout: float | None = 0.0) -> PushFrame | None:
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        with self._condition:
            while True:
                self._raise_gap_locked()
                if self._frames:
                    frame = self._frames.popleft()
                    self._bytes -= frame.wire_size
                    return frame
                if self._closed:
                    if self._error is not None:
                        raise self._error
                    return None
                if timeout is not None and timeout <= 0:
                    return None
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return None
                self._condition.wait(remaining)

    def drain(self) -> list[PushFrame]:
        with self._condition:
            self._raise_gap_locked()
            frames = list(self._frames)
            self._frames.clear()
            self._bytes = 0
            return frames

    def close(self, error: BaseException | None = None) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._error = error
            self._frames.clear()
            self._bytes = 0
            self._condition.notify_all()

    def close_before_deadline(self, deadline: float, error: BaseException | None = None) -> None:
        acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 push buffer close blocked before deadline")
        try:
            self.close(error)
        finally:
            self._condition.release()

    def abandon(self) -> None:
        self._closed = True
        acquired = self._condition.acquire(blocking=False)
        if not acquired:
            return
        try:
            self._frames.clear()
            self._bytes = 0
            self._condition.notify_all()
        finally:
            self._condition.release()

    def snapshot(self) -> PushBufferSnapshot:
        with self._condition:
            return PushBufferSnapshot(
                owner_epoch=self.owner_epoch,
                frame_count=len(self._frames),
                byte_count=self._bytes,
                max_frames_observed=self._max_frames_observed,
                max_bytes_observed=self._max_bytes_observed,
                dropped_total=self._dropped_total,
                gap_pending=self._gap_pending,
                closed=self._closed,
            )

    def snapshot_before_deadline(self, deadline: float) -> PushBufferSnapshot:
        acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 push buffer snapshot blocked before deadline")
        try:
            return self.snapshot()
        finally:
            self._condition.release()

    def _raise_gap_locked(self) -> None:
        if not self._gap_pending:
            return
        self._gap_pending = False
        raise PushOverflowError(f"7709 push gap detected; dropped_total={self._dropped_total}")
