from __future__ import annotations

import zlib

import pytest

from eltdx.exceptions import ProtocolError
from eltdx.protocol.constants import PREFIX_RESP
from eltdx.protocol.frame import ResponseFrameDecoder, decode_response


def response_bytes(msg_id: int, msg_type: int, payload: bytes, *, declared_length: int | None = None) -> bytes:
    length = len(payload) if declared_length is None else declared_length
    return (
        PREFIX_RESP
        + b"\x00"
        + msg_id.to_bytes(4, "little")
        + b"\x00"
        + msg_type.to_bytes(2, "little")
        + len(payload).to_bytes(2, "little")
        + length.to_bytes(2, "little")
        + payload
    )


@pytest.mark.parametrize("split", range(1, 20))
def test_decoder_retains_every_prefix_header_and_payload_split(split: int) -> None:
    raw = response_bytes(123, 0x044E, b"payload-data")
    decoder = ResponseFrameDecoder()

    frames = decoder.feed(raw[:split]) + decoder.feed(raw[split:])

    assert [(frame.msg_id, frame.msg_type, frame.data) for frame in frames] == [(123, 0x044E, b"payload-data")]
    assert decoder.buffered_bytes == 0


def test_decoder_accepts_one_byte_feeds_and_multiple_frames() -> None:
    raw = response_bytes(1, 4, b"one") + response_bytes(2, 5, b"two")
    decoder = ResponseFrameDecoder()

    frames = []
    for value in raw:
        frames.extend(decoder.feed(bytes((value,))))

    assert [(frame.msg_id, frame.data) for frame in frames] == [(1, b"one"), (2, b"two")]
    assert decoder.finish() == []


def test_decoder_resynchronizes_and_keeps_partial_prefix_suffix() -> None:
    decoder = ResponseFrameDecoder(max_resync_bytes=16)
    assert decoder.feed(b"garbage" + PREFIX_RESP[:3]) == []
    assert decoder.buffered_bytes == 3

    frames = decoder.feed(response_bytes(7, 8, b"ok")[3:])

    assert [frame.data for frame in frames] == [b"ok"]
    assert decoder.resync_discarded == 7


def test_decoder_rejects_resync_and_payload_limits() -> None:
    decoder = ResponseFrameDecoder(max_payload_size=4, max_buffer_size=20, max_resync_bytes=4)
    with pytest.raises(ProtocolError, match="resync exceeds limit"):
        decoder.feed(b"12345")

    decoder = ResponseFrameDecoder(max_payload_size=4, max_buffer_size=20)
    with pytest.raises(ProtocolError, match="compressed payload exceeds limit"):
        decoder.feed(response_bytes(1, 2, b"12345"))


def test_decoder_buffer_never_exceeds_configured_limit_for_large_garbage_feed() -> None:
    decoder = ResponseFrameDecoder(max_payload_size=16, max_buffer_size=32, max_resync_bytes=1000)

    assert decoder.feed(b"x" * 1000) == []

    assert decoder.max_buffer_observed <= 32
    assert decoder.resync_discarded == 1000


def test_decoder_rejects_truncated_frame_at_eof() -> None:
    decoder = ResponseFrameDecoder()
    decoder.feed(response_bytes(1, 2, b"abc")[:-1])

    with pytest.raises(ProtocolError, match="truncated response frame"):
        decoder.finish()


def test_safe_zlib_decode_accepts_exact_stream() -> None:
    data = b"A" * 200
    raw = response_bytes(1, 2, zlib.compress(data), declared_length=len(data))

    assert decode_response(raw).data == data


@pytest.mark.parametrize(
    ("payload", "length", "message"),
    [
        (b"not-zlib", 100, "invalid compressed"),
        (zlib.compress(b"abc")[:-1], 3, "before zlib stream EOF"),
        (zlib.compress(b"abc") + b"tail", 3, "trailing data"),
        (zlib.compress(b"abcdef"), 5, "exceeds declared length"),
        (zlib.compress(b"abc"), 4, "decoded length mismatch"),
    ],
)
def test_safe_zlib_decode_rejects_malformed_or_wrong_length(payload: bytes, length: int, message: str) -> None:
    with pytest.raises(ProtocolError, match=message):
        decode_response(response_bytes(1, 2, payload, declared_length=length))


def test_safe_zlib_decode_enforces_configured_output_limit_before_decompression() -> None:
    data = b"A" * 100
    raw = response_bytes(1, 2, zlib.compress(data), declared_length=len(data))

    with pytest.raises(ProtocolError, match="decoded payload exceeds limit"):
        decode_response(raw, max_payload_size=32)
