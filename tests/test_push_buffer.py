from __future__ import annotations

import inspect
import sys
import threading

import pytest

from eltdx.exceptions import PushOverflowError, TransportError
from eltdx.protocol.frame import ResponseFrame
from eltdx.transport.push import PushBuffer, PushFrame


def frame(epoch: int, msg_id: int, size: int = 1) -> PushFrame:
    response = ResponseFrame(0, msg_id, 0x0547, size, size, b"x" * size, b"x" * (16 + size))
    return PushFrame(epoch, 1, "127.0.0.1:7709", response)


def test_push_buffer_enforces_frame_and_byte_limits_with_one_sticky_gap() -> None:
    buffer = PushBuffer(1, max_frames=2, max_bytes=36)
    assert buffer.offer_nowait(frame(1, 1))
    assert buffer.offer_nowait(frame(1, 2))
    assert buffer.offer_nowait(frame(1, 3))

    with pytest.raises(PushOverflowError, match="dropped_total=1"):
        buffer.poll()
    assert [buffer.poll().response.msg_id, buffer.poll().response.msg_id] == [2, 3]
    assert buffer.poll() is None
    snapshot = buffer.snapshot()
    assert snapshot.dropped_total == 1
    assert snapshot.max_frames_observed <= 2
    assert snapshot.max_bytes_observed <= 36


def test_oversized_and_wrong_epoch_frames_never_enter_buffer() -> None:
    buffer = PushBuffer(2, max_frames=2, max_bytes=20)
    assert not buffer.offer_nowait(frame(1, 1))
    assert not buffer.offer_nowait(frame(2, 2, size=5))
    with pytest.raises(PushOverflowError, match="dropped_total=1"):
        buffer.drain()
    assert buffer.drain() == []


@pytest.mark.parametrize("error", [None, TransportError("fatal")])
def test_close_wakes_all_blocking_pollers(error: BaseException | None) -> None:
    buffer = PushBuffer(3)
    started = threading.Barrier(3)
    results = []

    def poll() -> None:
        started.wait(timeout=2)
        try:
            results.append(buffer.poll(None))
        except BaseException as exc:
            results.append(exc)

    threads = [threading.Thread(target=poll) for _ in range(2)]
    for thread in threads:
        thread.start()
    started.wait(timeout=2)
    buffer.close(error)
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()
    if error is None:
        assert results == [None, None]
    else:
        assert len(results) == 2 and all(item is error for item in results)


def test_close_rejects_late_offer_and_discards_old_frames() -> None:
    buffer = PushBuffer(4)
    buffer.offer_nowait(frame(4, 1))
    buffer.close()

    assert not buffer.offer_nowait(frame(4, 2))
    assert buffer.poll() is None
    assert buffer.snapshot().frame_count == 0


def test_new_actor_drop_cannot_be_cleared_with_an_older_reported_gap() -> None:
    class PausingGapBuffer(PushBuffer):
        armed = False

        def __setattr__(self, name, value) -> None:
            pause = self.__dict__.get("pause")
            resume = self.__dict__.get("resume")
            target = (name == "_gap_pending" and value is False) or (
                name == "_reported_dropped_total" and value == 1
            )
            if self.armed and target and pause is not None and resume is not None:
                pause.set()
                assert resume.wait(timeout=2)
            super().__setattr__(name, value)

    buffer = PausingGapBuffer(4)
    buffer.pause = threading.Event()
    buffer.resume = threading.Event()
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with buffer._condition:
            condition_held.set()
            release_condition.wait()

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    assert not buffer.offer_nowait(frame(4, 1))
    release_condition.set()
    holder.join(timeout=2)

    first_errors: list[BaseException] = []
    buffer.armed = True

    def report_first_gap() -> None:
        try:
            buffer.poll()
        except BaseException as exc:
            first_errors.append(exc)

    reporter = threading.Thread(target=report_first_gap)
    reporter.start()
    assert buffer.pause.wait(timeout=2)
    assert not buffer.offer_nowait(frame(4, 2))
    buffer.resume.set()
    reporter.join(timeout=2)

    assert len(first_errors) == 1 and isinstance(first_errors[0], PushOverflowError)
    with pytest.raises(PushOverflowError, match="dropped_total=2"):
        buffer.poll()


def test_close_publication_between_poll_check_and_registration_is_not_lost() -> None:
    buffer = PushBuffer(5)
    source, first_line = inspect.getsourcelines(buffer.poll)
    pause_line = first_line + next(
        index for index, line in enumerate(source) if "if waiter is None" in line
    )
    paused = threading.Event()
    resume = threading.Event()
    results: list[object] = []

    def trace(frame, event, _arg):
        if event == "line" and frame.f_code is buffer.poll.__func__.__code__ and frame.f_lineno == pause_line:
            paused.set()
            assert resume.wait(timeout=2)
        return trace

    def poll() -> None:
        sys.settrace(trace)
        try:
            results.append(buffer.poll(None))
        finally:
            sys.settrace(None)

    poller = threading.Thread(target=poll)
    poller.start()
    assert paused.wait(timeout=2)
    buffer.publish_close()
    resume.set()
    poller.join(timeout=0.2)

    assert not poller.is_alive()
    assert results == [None]


def test_close_publication_exposes_error_before_terminal_flag() -> None:
    class PausingCloseBuffer(PushBuffer):
        armed = False

        def __setattr__(self, name, value) -> None:
            super().__setattr__(name, value)
            if self.armed and name == "_close_published" and value is True:
                self.published.set()
                assert self.resume.wait(timeout=2)

    buffer = PausingCloseBuffer(6)
    buffer.published = threading.Event()
    buffer.resume = threading.Event()
    buffer.armed = True
    error = RuntimeError("fatal push close")
    publisher = threading.Thread(target=lambda: buffer.publish_close(error))
    publisher.start()
    assert buffer.published.wait(timeout=2)
    try:
        with pytest.raises(RuntimeError, match="fatal push close") as raised:
            buffer.poll()
        assert raised.value is error
    finally:
        buffer.resume.set()
        publisher.join(timeout=2)
