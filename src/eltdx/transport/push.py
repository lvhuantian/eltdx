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


@dataclass(slots=True, eq=False)
class PushDropPublication:
    """Monotonic drop count written by one Actor and read by the owner."""

    total: int = 0
    observed: int = 0


class PushBuffer:
    def __init__(
        self,
        owner_epoch: int,
        *,
        max_frames: int = 1024,
        max_bytes: int = 8 * 1024 * 1024,
        retire_event: threading.Event | None = None,
    ) -> None:
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
        self._reported_dropped_total = 0
        self._fallback_drop_publication = PushDropPublication()
        self._drop_publications: tuple[PushDropPublication, ...] = (
            self._fallback_drop_publication,
        )
        self._retire_event = retire_event or threading.Event()
        self._close_requested = threading.Event()
        self._closed = False
        self._drained = False
        self._error: BaseException | None = None
        self._close_published = False
        self._published_error: BaseException | None = None
        self._waiters: set[threading.Event] = set()
        self._waiter_snapshot: tuple[threading.Event, ...] = ()

    def bind_retire_event(self, retire_event: threading.Event) -> None:
        previous = self._retire_event
        self._retire_event = retire_event
        if previous is not retire_event and previous.is_set():
            retire_event.set()

    def _termination_requested(self) -> bool:
        return self._retire_event.is_set() or self._close_requested.is_set()

    def _drain_closed_locked(self, error: BaseException | None = None) -> None:
        self._sync_drop_publications_locked()
        if self._error is None:
            self._error = error or self._published_error
        self._frames.clear()
        self._bytes = 0
        self._closed = True
        self._drained = True
        self._condition.notify_all()

    @property
    def pending_count(self) -> int:
        with self._condition:
            return len(self._frames)

    def register_drop_publication(
        self,
        publication: PushDropPublication,
        *,
        deadline: float | None = None,
    ) -> bool:
        if deadline is None:
            self._condition.acquire()
            acquired = True
        else:
            acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            return False
        try:
            if self._termination_requested():
                return False
            if all(item is not publication for item in self._drop_publications):
                self._drop_publications = self._drop_publications + (publication,)
            if self._termination_requested():
                self._drop_publications = tuple(
                    item for item in self._drop_publications if item is not publication
                )
                return False
            return True
        finally:
            self._condition.release()

    def offer_nowait(
        self,
        frame: PushFrame,
        *,
        drop_publication: PushDropPublication | None = None,
    ) -> bool:
        if frame.runtime_epoch != self.owner_epoch or self._termination_requested():
            return False
        size = frame.wire_size
        if not self._condition.acquire(blocking=False):
            if self._termination_requested():
                return False
            publication = drop_publication or self._fallback_drop_publication
            publication.total += 1
            for waiter in self._waiter_snapshot:
                waiter.set()
            return False
        wake: tuple[threading.Event, ...] = ()
        try:
            self._sync_drop_publications_locked()
            if self._termination_requested() or self._closed:
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
            if self._termination_requested():
                if accepted:
                    if self._frames and self._frames[-1] is frame:
                        self._frames.pop()
                        self._bytes -= size
                return False
            if dropped:
                self._dropped_total += dropped
            self._max_frames_observed = max(self._max_frames_observed, len(self._frames))
            self._max_bytes_observed = max(self._max_bytes_observed, self._bytes)
            self._condition.notify()
            wake = self._waiter_snapshot
        finally:
            self._condition.release()
        for waiter in wake:
            waiter.set()
        return accepted

    def poll(self, timeout: float | None = 0.0) -> PushFrame | None:
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        waiter: threading.Event | None = None
        try:
            while True:
                with self._condition:
                    self._sync_drop_publications_locked()
                    if self._published_error is not None:
                        raise self._published_error
                    if self._error is not None:
                        raise self._error
                    if self._retire_event.is_set():
                        return None
                    self._raise_gap_locked()
                    if self._frames:
                        frame = self._frames.popleft()
                        self._bytes -= frame.wire_size
                        if self._published_error is not None:
                            error = self._published_error
                            self._drain_closed_locked(error)
                            raise error
                        if self._error is not None:
                            error = self._error
                            self._drain_closed_locked(error)
                            raise error
                        if self._retire_event.is_set():
                            self._drain_closed_locked()
                            return None
                        return frame
                    if self._close_published or self._closed or self._close_requested.is_set():
                        return None
                    if timeout is not None and timeout <= 0:
                        return None
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        return None
                    if waiter is None:
                        waiter = threading.Event()
                        self._waiters.add(waiter)
                        self._waiter_snapshot = tuple(self._waiters)
                    waiter.clear()
                    self._sync_drop_publications_locked()
                    if (
                        self._dropped_total > self._reported_dropped_total
                        or self._termination_requested()
                        or self._close_published
                        or self._closed
                        or bool(self._frames)
                    ):
                        waiter.set()
                waiter.wait(remaining)
        finally:
            if waiter is not None:
                with self._condition:
                    self._waiters.discard(waiter)
                    self._waiter_snapshot = tuple(self._waiters)

    def drain(self) -> list[PushFrame]:
        with self._condition:
            if self._published_error is not None:
                raise self._published_error
            if self._retire_event.is_set():
                self._drain_closed_locked()
                return []
            self._raise_gap_locked()
            frames = list(self._frames)
            if self._published_error is not None:
                error = self._published_error
                self._drain_closed_locked(error)
                raise error
            if self._error is not None:
                error = self._error
                self._drain_closed_locked(error)
                raise error
            if self._retire_event.is_set():
                self._drain_closed_locked()
                return []
            self._frames.clear()
            self._bytes = 0
            return frames

    def close(self, error: BaseException | None = None) -> None:
        self.publish_close(error)
        with self._condition:
            self._drain_closed_locked(error)
        for waiter in self._waiter_snapshot:
            waiter.set()

    def publish_close(self, error: BaseException | None = None) -> None:
        if self._published_error is None:
            self._published_error = error
        self._close_published = True
        self._close_requested.set()
        for waiter in self._waiter_snapshot:
            waiter.set()

    def close_before_deadline(self, deadline: float, error: BaseException | None = None) -> None:
        self.publish_close(error)
        acquired = self._condition.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 push buffer close blocked before deadline")
        try:
            self.close(error)
        finally:
            self._condition.release()

    def abandon(self, error: BaseException | None = None) -> None:
        self.publish_close(error)
        acquired = self._condition.acquire(blocking=False)
        if not acquired:
            return
        try:
            self._drain_closed_locked(error)
        finally:
            self._condition.release()

    def snapshot(self) -> PushBufferSnapshot:
        with self._condition:
            self._sync_drop_publications_locked()
            return PushBufferSnapshot(
                owner_epoch=self.owner_epoch,
                frame_count=len(self._frames),
                byte_count=self._bytes,
                max_frames_observed=self._max_frames_observed,
                max_bytes_observed=self._max_bytes_observed,
                dropped_total=self._dropped_total,
                gap_pending=self._dropped_total > self._reported_dropped_total,
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
        self._sync_drop_publications_locked()
        if self._dropped_total <= self._reported_dropped_total:
            return
        self._reported_dropped_total = self._dropped_total
        raise PushOverflowError(f"7709 push gap detected; dropped_total={self._dropped_total}")

    def _sync_drop_publications_locked(self) -> None:
        for publication in self._drop_publications:
            total = publication.total
            if total <= publication.observed:
                continue
            self._dropped_total += total - publication.observed
            publication.observed = total
