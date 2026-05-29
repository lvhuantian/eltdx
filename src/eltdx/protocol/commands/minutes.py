"""Intraday minute command builders and parsers."""

from __future__ import annotations

from datetime import date, datetime

from eltdx.exceptions import ProtocolError
from eltdx.models import MinuteAuxPoint, MinuteAuxSeries, MinutePoint, MinuteSeries, SparklineSeries
from eltdx.protocol.constants import TYPE_HISTORICAL_INTRADAY, TYPE_INTRADAY_AUX, TYPE_RECENT_INTRADAY, TYPE_SPARKLINE, TYPE_TODAY_INTRADAY
from eltdx.protocol.frame import RequestFrame, ResponseFrame
from eltdx.protocol.unit import (
    consume_price,
    consume_varint,
    date_from_yyyymmdd,
    little_f32,
    little_u16,
    little_u32,
    market_to_id,
    milli_to_float,
    minute_index_datetime,
    minute_index_label,
    split_code,
    yyyymmdd,
)

RECENT_DATE_SELECTOR_BASE = 0xFED62304
INTRADAY_AUX_SELECTORS = {
    "buy_sell_strength": 0x00,
    "buy_sell": 0x00,
    "commission": 0x00,
    "volume_comparison": 0x0B,
    "volume_compare": 0x0B,
}


def build_today_intraday_frame(payload: dict, msg_id: int) -> RequestFrame:
    market_id, _, number = split_code(payload["code"])
    reserved_tail = payload.get("reserved_tail_raw", bytes.fromhex("00000093"))
    if isinstance(reserved_tail, str):
        reserved_tail = bytes.fromhex(reserved_tail.replace(" ", ""))
    if len(reserved_tail) != 4:
        raise ValueError("reserved_tail_raw must be 4 bytes")
    data = market_id.to_bytes(2, "little", signed=False) + number.encode("ascii") + reserved_tail
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_TODAY_INTRADAY, data=data)


def parse_today_intraday_payload(response: ResponseFrame, request_payload: dict | None = None) -> MinuteSeries:
    request_payload = request_payload or {}
    payload = response.data
    if len(payload) < 4:
        raise ProtocolError("invalid today intraday payload")

    market_id, exchange, number = split_code(request_payload.get("code", "sz000001"))
    count = little_u16(payload[:2])
    reserved_zero = little_u16(payload[2:4])
    offset = 4
    points: list[MinutePoint] = []
    first_price = None
    first_avg = None
    for index in range(count):
        record_start = offset
        price_field, offset = consume_price(payload, offset)
        avg_field, offset = consume_price(payload, offset)
        volume, offset = consume_varint(payload, offset)
        if first_price is None:
            first_price = price_field
            first_avg = avg_field
        assert first_avg is not None
        price_raw = price_field if index == 0 else first_price + price_field
        avg_raw = avg_field if index == 0 else first_avg + avg_field
        price_milli = price_raw * 10
        points.append(
            MinutePoint(
                index=index,
                time_label=minute_index_label(index),
                time=None,
                price=milli_to_float(price_milli),
                price_milli=price_milli,
                volume=volume,
                price_field=price_field,
                avg_field=avg_field,
                avg_price=avg_raw / 10000.0,
                price_raw=price_raw,
                avg_raw=avg_raw,
                record_hex=payload[record_start:offset].hex(),
            )
        )
    if offset != len(payload):
        raise ProtocolError(f"unexpected trailing today intraday payload bytes: {len(payload) - offset}")
    return MinuteSeries(
        exchange=exchange,
        market_id=market_id,
        code=number,
        trading_date=None,
        points=tuple(points),
        reserved_zero=reserved_zero,
        raw_payload=payload,
    )


def build_historical_intraday_frame(payload: dict, msg_id: int) -> RequestFrame:
    market_id, _, number = split_code(payload["code"])
    trading_date_raw = yyyymmdd(payload.get("trading_date"))
    data = trading_date_raw.to_bytes(4, "little", signed=False) + bytes([market_id]) + number.encode("ascii")
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_HISTORICAL_INTRADAY, data=data)


def parse_historical_intraday_payload(response: ResponseFrame, request_payload: dict | None = None) -> MinuteSeries:
    request_payload = request_payload or {}
    payload = response.data
    if len(payload) < 6:
        raise ProtocolError("invalid historical intraday payload")

    market_id, exchange, number = split_code(request_payload.get("code", "sz000001"))
    trading_date_raw = yyyymmdd(request_payload.get("trading_date"))
    trading_day = date_from_yyyymmdd(trading_date_raw)
    if trading_day is None:
        raise ProtocolError(f"invalid trading date: {trading_date_raw}")

    count = little_u16(payload[:2])
    prev_close = little_f32(payload[2:6])
    offset = 6
    price_acc_raw = 0
    points: list[MinutePoint] = []
    for index in range(count):
        record_start = offset
        price_delta_raw, offset = consume_price(payload, offset)
        aux_delta_raw, offset = consume_price(payload, offset)
        volume, offset = consume_varint(payload, offset)
        price_acc_raw += price_delta_raw
        price_milli = price_acc_raw * 10
        points.append(
            MinutePoint(
                index=index,
                time_label=minute_index_label(index),
                time=minute_index_datetime(trading_day, index),
                price=milli_to_float(price_milli),
                price_milli=price_milli,
                volume=volume,
                price_delta_raw=price_delta_raw,
                aux_delta_raw=aux_delta_raw,
                price_raw=price_acc_raw,
                record_hex=payload[record_start:offset].hex(),
            )
        )
    if offset != len(payload):
        raise ProtocolError(f"unexpected trailing historical intraday payload bytes: {len(payload) - offset}")
    return MinuteSeries(
        exchange=exchange,
        market_id=market_id,
        code=number,
        trading_date=trading_day,
        points=tuple(points),
        prev_close=prev_close,
        raw_payload=payload,
    )


def build_recent_intraday_frame(payload: dict, msg_id: int) -> RequestFrame:
    market_id, _, number = split_code(payload["code"])
    trading_date_raw = yyyymmdd(payload.get("trading_date"))
    trading_day = date_from_yyyymmdd(trading_date_raw)
    if trading_day is None:
        raise ProtocolError(f"invalid trading date: {trading_date_raw}")
    date_selector_raw = RECENT_DATE_SELECTOR_BASE - trading_day.toordinal()
    data = date_selector_raw.to_bytes(4, "little") + bytes([market_id]) + number.encode("ascii")
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_RECENT_INTRADAY, data=data)


def parse_recent_intraday_payload(response: ResponseFrame, request_payload: dict | None = None) -> MinuteSeries:
    request_payload = request_payload or {}
    payload = response.data
    if len(payload) < 10:
        raise ProtocolError("invalid recent intraday payload")

    market_id, exchange, number = split_code(request_payload.get("code", "sz000001"))
    trading_date_raw = yyyymmdd(request_payload.get("trading_date"))
    trading_day = date_from_yyyymmdd(trading_date_raw)
    if trading_day is None:
        raise ProtocolError(f"invalid trading date: {trading_date_raw}")
    date_selector_raw = RECENT_DATE_SELECTOR_BASE - trading_day.toordinal()

    count = little_u16(payload[:2])
    prev_close = little_f32(payload[2:6])
    open_price = little_f32(payload[6:10])
    offset = 10
    points: list[MinutePoint] = []
    first_price = None
    first_avg = None
    for index in range(count):
        record_start = offset
        price_field, offset = consume_price(payload, offset)
        avg_field, offset = consume_price(payload, offset)
        volume, offset = consume_varint(payload, offset)
        if first_price is None:
            first_price = price_field
            first_avg = avg_field
        assert first_avg is not None
        price_raw = price_field if index == 0 else first_price + price_field
        avg_raw = avg_field if index == 0 else first_avg + avg_field
        price_milli = price_raw * 10
        points.append(
            MinutePoint(
                index=index,
                time_label=minute_index_label(index),
                time=minute_index_datetime(trading_day, index),
                price=milli_to_float(price_milli),
                price_milli=price_milli,
                volume=volume,
                price_field=price_field,
                avg_field=avg_field,
                avg_price=avg_raw / 10000.0,
                price_raw=price_raw,
                avg_raw=avg_raw,
                record_hex=payload[record_start:offset].hex(),
            )
        )
    if offset != len(payload):
        raise ProtocolError(f"unexpected trailing recent intraday payload bytes: {len(payload) - offset}")
    return MinuteSeries(
        exchange=exchange,
        market_id=market_id,
        code=number,
        trading_date=trading_day,
        points=tuple(points),
        prev_close=prev_close,
        open_price=open_price,
        date_selector_raw=date_selector_raw,
        raw_payload=payload,
    )


def build_intraday_aux_frame(payload: dict, msg_id: int) -> RequestFrame:
    market_id, _, number = split_code(payload["code"])
    selector_raw = normalize_intraday_aux_selector(payload.get("selector", payload.get("kind", 0)))
    data = market_id.to_bytes(2, "little") + number.encode("ascii") + b"\x00" * 19 + bytes([selector_raw])
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_INTRADAY_AUX, data=data)


def parse_intraday_aux_payload(response: ResponseFrame, request_payload: dict | None = None) -> MinuteAuxSeries:
    request_payload = request_payload or {}
    payload = response.data
    if len(payload) < 2:
        raise ProtocolError("invalid intraday aux payload")

    market_id, exchange, number = split_code(request_payload.get("code", "sz000001"))
    selector_raw = normalize_intraday_aux_selector(request_payload.get("selector", request_payload.get("kind", 0)))
    count = little_u16(payload[:2])
    offset = 2
    points: list[MinuteAuxPoint] = []
    if selector_raw == 0x0B:
        expected_length = 2 + count * 8
        if len(payload) != expected_length:
            raise ProtocolError(f"invalid intraday volume comparison length: expected {expected_length}, got {len(payload)}")
        for index in range(count):
            record_start = offset
            previous_day = little_f32(payload[offset : offset + 4])
            current_day = little_f32(payload[offset + 4 : offset + 8])
            offset += 8
            points.append(
                MinuteAuxPoint(
                    index=index,
                    time_label=minute_index_label(index),
                    series_a=previous_day,
                    series_b=current_day,
                    previous_day_cumulative_volume=previous_day,
                    current_day_cumulative_volume=current_day,
                    cumulative_volume=current_day,
                    record_hex=payload[record_start:offset].hex(),
                )
            )
        kind = "volume_comparison"
    else:
        for index in range(count):
            record_start = offset
            series_a, offset = consume_varint(payload, offset)
            series_b, offset = consume_varint(payload, offset)
            points.append(
                MinuteAuxPoint(
                    index=index,
                    time_label=minute_index_label(index),
                    series_a=series_a,
                    series_b=series_b,
                    buy_commission=series_a,
                    sell_commission=series_b,
                    record_hex=payload[record_start:offset].hex(),
                )
            )
        if offset != len(payload):
            raise ProtocolError(f"unexpected trailing intraday aux payload bytes: {len(payload) - offset}")
        kind = "buy_sell_strength"

    return MinuteAuxSeries(
        exchange=exchange,
        market_id=market_id,
        code=number,
        selector_raw=selector_raw,
        kind=kind,
        points=tuple(points),
        raw_payload=payload,
    )


def build_sparkline_frame(payload: dict, msg_id: int) -> RequestFrame:
    market_id, _, number = split_code(payload["code"])
    selector = int(payload.get("selector", 1))
    window = int(payload.get("window", payload.get("window_or_count_raw", 20)))
    fixed_raw = int(payload.get("fixed_raw", 0x01000000))
    data = (
        bytes([market_id, 0])
        + number.encode("ascii")
        + b"\x00" * 16
        + bytes([selector, 0])
        + window.to_bytes(2, "little")
        + fixed_raw.to_bytes(4, "little")
        + b"\x00" * 5
    )
    return RequestFrame(msg_id=msg_id, msg_type=TYPE_SPARKLINE, data=data)


def parse_sparkline_payload(response: ResponseFrame, request_payload: dict | None = None) -> SparklineSeries:
    request_payload = request_payload or {}
    payload = response.data
    if len(payload) < 42:
        raise ProtocolError("invalid sparkline payload")

    request_market_id, request_exchange, request_number = split_code(request_payload.get("code", "sz000001"))
    market_id = payload[0]
    code = payload[2:8].decode("ascii")
    selector_echo = payload[24]
    reserved_param_u32 = little_u32(payload[26:30])
    max_count_raw = little_u16(payload[34:36])
    base_price = little_f32(payload[36:40])
    price_count = little_u16(payload[40:42])
    expected_length = 42 + price_count * 4
    if len(payload) != expected_length:
        raise ProtocolError(f"invalid sparkline length: expected {expected_length}, got {len(payload)}")

    prices = []
    offset = 42
    for _ in range(price_count):
        prices.append(little_f32(payload[offset : offset + 4]))
        offset += 4

    exchange = {0: "sz", 1: "sh", 2: "bj"}.get(market_id, request_exchange)
    return SparklineSeries(
        exchange=exchange,
        market_id=market_id,
        code=code or request_number,
        selector_raw=int(request_payload.get("selector", 1)),
        selector_echo=selector_echo,
        window_or_count_raw=int(request_payload.get("window", request_payload.get("window_or_count_raw", 20))),
        max_count_raw=max_count_raw,
        base_price=base_price,
        prices=tuple(prices),
        reserved_param_u32=reserved_param_u32,
        raw_payload=payload,
    )


def normalize_intraday_aux_selector(value) -> int:
    if isinstance(value, int):
        return value
    key = str(value).strip().lower()
    if key in INTRADAY_AUX_SELECTORS:
        return INTRADAY_AUX_SELECTORS[key]
    if key.startswith("0x"):
        return int(key, 16)
    return int(key)
