"""Call auction command builder and parser."""

from __future__ import annotations

from eltdx.exceptions import ProtocolError
from eltdx.models import AuctionPoint, AuctionSeries
from eltdx.protocol.constants import TYPE_AUCTION_SERIES
from eltdx.protocol.frame import RequestFrame, ResponseFrame
from eltdx.protocol.unit import little_f32, little_u16, little_u32, split_code

from .trades import minute_of_day_label

AUCTION_RECORD_SIZE = 16


def build_auction_series_frame(payload: dict, msg_id: int) -> RequestFrame:
    market_id, _, number = split_code(payload["code"])
    mode_or_selector_raw = int(payload.get("mode_or_selector_raw", payload.get("selector", 3)))
    start_raw = int(payload.get("start_raw", payload.get("start", 0)))
    limit_or_count_raw = int(payload.get("limit_or_count_raw", payload.get("limit", 500)))
    data = (
        bytes([market_id, 0])
        + number.encode("ascii")
        + (0).to_bytes(4, "little")
        + mode_or_selector_raw.to_bytes(4, "little")
        + (0).to_bytes(4, "little")
        + start_raw.to_bytes(4, "little")
        + limit_or_count_raw.to_bytes(4, "little")
    )
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_AUCTION_SERIES, data=data)


def parse_auction_series_payload(response: ResponseFrame, request_payload: dict | None = None) -> AuctionSeries:
    request_payload = request_payload or {}
    payload = response.data
    if len(payload) < 2:
        raise ProtocolError("invalid auction series payload")

    market_id, exchange, number = split_code(request_payload.get("code", "sz000001"))
    count = little_u16(payload[:2])
    expected_length = 2 + count * AUCTION_RECORD_SIZE
    if len(payload) != expected_length:
        raise ProtocolError(f"invalid auction series length: expected {expected_length}, got {len(payload)}")

    points: list[AuctionPoint] = []
    offset = 2
    for index in range(count):
        record = payload[offset : offset + AUCTION_RECORD_SIZE]
        offset += AUCTION_RECORD_SIZE
        minute_of_day_raw = little_u16(record[:2])
        price = little_f32(record[2:6])
        matched_volume = little_u32(record[6:10])
        unmatched_signed_raw = int.from_bytes(record[10:14], "little", signed=True)
        reserved_zero_0e = record[14]
        second_raw = record[15]
        points.append(
            AuctionPoint(
                index=index,
                minute_of_day_raw=minute_of_day_raw,
                second_raw=second_raw,
                time_label=minute_of_day_label(minute_of_day_raw, with_seconds=second_raw),
                time_seconds=minute_of_day_raw * 60 + second_raw,
                price=price,
                price_milli=round(price * 1000),
                matched_volume=matched_volume,
                unmatched_signed_raw=unmatched_signed_raw,
                unmatched_volume=abs(unmatched_signed_raw),
                unmatched_direction_raw=1 if unmatched_signed_raw >= 0 else -1,
                reserved_zero_0e=reserved_zero_0e,
                record_hex=record.hex(),
            )
        )

    return AuctionSeries(
        exchange=exchange,
        market_id=market_id,
        code=number,
        mode_or_selector_raw=int(request_payload.get("mode_or_selector_raw", request_payload.get("selector", 3))),
        start_raw=int(request_payload.get("start_raw", request_payload.get("start", 0))),
        limit_or_count_raw=int(request_payload.get("limit_or_count_raw", request_payload.get("limit", 500))),
        points=tuple(points),
        raw_payload=payload,
    )
