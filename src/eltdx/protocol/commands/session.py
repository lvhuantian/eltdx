"""Session command builders and parsers."""

from __future__ import annotations

from datetime import datetime

from eltdx.exceptions import ProtocolError
from eltdx.models import HandshakeInfo, HeartbeatAck
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_HEARTBEAT
from eltdx.protocol.frame import RequestFrame, ResponseFrame
from eltdx.protocol.unit import date_from_yyyymmdd, decode_gbk_text, little_u16, little_u32


def build_handshake_frame(payload: dict, msg_id: int) -> RequestFrame:
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_HANDSHAKE, data=b"\x01")


def parse_handshake_payload(response: ResponseFrame) -> HandshakeInfo:
    payload = response.data
    if len(payload) < 189:
        raise ProtocolError(f"invalid handshake payload length: {len(payload)}")

    server_datetime = _parse_server_datetime(payload)
    session_minutes_1 = _parse_session_minutes(payload[9:25])
    session_minutes_2 = _parse_session_minutes(payload[25:41])
    date_1_raw = little_u32(payload[42:46])
    date_2_raw = little_u32(payload[50:54])

    return HandshakeInfo(
        server_datetime=server_datetime,
        session_minutes_1=session_minutes_1,
        session_minutes_2=session_minutes_2,
        server_date_1=date_from_yyyymmdd(date_1_raw),
        server_date_2=date_from_yyyymmdd(date_2_raw),
        server_name=decode_gbk_text(payload[68:152]),
        product_tag=decode_gbk_text(payload[160:189]),
        unknown_time_1_raw=little_u32(payload[46:50]),
        unknown_time_2_raw=little_u32(payload[54:58]),
        flags_raw=payload[58:68],
        tail_control_raw=payload[152:160],
        raw_payload=payload,
    )


def build_heartbeat_frame(payload: dict, msg_id: int) -> RequestFrame:
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_HEARTBEAT)


def parse_heartbeat_payload(response: ResponseFrame) -> HeartbeatAck:
    payload = response.data
    if len(payload) < 10:
        raise ProtocolError(f"invalid heartbeat payload length: {len(payload)}")

    server_date_raw = little_u32(payload[6:10])
    return HeartbeatAck(
        reserved=payload[:6],
        server_date_raw=server_date_raw,
        server_date=date_from_yyyymmdd(server_date_raw),
        raw_payload=payload,
    )


def _parse_server_datetime(payload: bytes) -> datetime | None:
    try:
        return datetime(
            little_u16(payload[1:3]),
            payload[4],
            payload[3],
            payload[6],
            payload[5],
            payload[8],
        )
    except (ValueError, IndexError):
        return None


def _parse_session_minutes(payload: bytes) -> tuple[str, ...]:
    values = []
    for offset in range(0, min(len(payload), 16), 2):
        minute = little_u16(payload[offset : offset + 2])
        values.append(f"{minute // 60:02d}:{minute % 60:02d}")
    return tuple(values)
