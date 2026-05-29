"""Special limit-price command builder and parser."""

from __future__ import annotations

from eltdx.exceptions import ProtocolError
from eltdx.models import SpecialLimitPage, SpecialLimitRecord
from eltdx.protocol.constants import TYPE_SPECIAL_LIMITS
from eltdx.protocol.frame import RequestFrame, ResponseFrame
from eltdx.protocol.unit import little_f32, little_u16, little_u32

LIMIT_RECORD_SIZE = 13


def build_special_limits_frame(payload: dict, msg_id: int) -> RequestFrame:
    start_index = int(payload.get("start_index", 0))
    if start_index < 0 or start_index > 0xFFFF:
        raise ValueError("start_index must be between 0 and 65535")
    data = start_index.to_bytes(2, "little") + b"\x00" * 12
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_SPECIAL_LIMITS, data=data)


def parse_special_limits_payload(response: ResponseFrame, request_payload: dict | None = None) -> SpecialLimitPage:
    request_payload = request_payload or {}
    payload = response.data
    if len(payload) < 2:
        raise ProtocolError("invalid special limits payload")
    count = little_u16(payload[:2])
    expected_length = 2 + count * LIMIT_RECORD_SIZE
    if len(payload) != expected_length:
        raise ProtocolError(f"invalid special limits length: expected {expected_length}, got {len(payload)}")

    records: list[SpecialLimitRecord] = []
    offset = 2
    for _ in range(count):
        record = payload[offset : offset + LIMIT_RECORD_SIZE]
        offset += LIMIT_RECORD_SIZE
        market_id = record[0]
        exchange = {0: "sz", 1: "sh", 2: "bj"}.get(market_id, "unknown")
        code_num = little_u32(record[1:5])
        records.append(
            SpecialLimitRecord(
                exchange=exchange,
                market_id=market_id,
                code_num=code_num,
                code=f"{code_num:06d}",
                upper_price_raw_f32=little_f32(record[5:9]),
                lower_price_raw_f32=little_f32(record[9:13]),
                record_hex=record.hex(),
            )
        )

    return SpecialLimitPage(
        start_index=int(request_payload.get("start_index", 0)),
        records=tuple(records),
        raw_payload=payload,
    )
