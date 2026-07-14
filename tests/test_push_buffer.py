from __future__ import annotations

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
