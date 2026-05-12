from __future__ import annotations

from typing import Any

from .adjustment import normalize_adjust_mode
from .client import TdxClient
from .serialization import to_jsonable


def _normalize_codes(codes: str | list[str] | tuple[str, ...]) -> list[str]:
    if isinstance(codes, str):
        return [item.strip() for item in codes.split(",") if item.strip()]
    return [str(item).strip() for item in codes if str(item).strip()]


def _normalize_adjust(adjust: str | None) -> str | None:
    if adjust is None:
        return None
    text = str(adjust).strip()
    if not text or text.lower() == "none":
        return None
    try:
        return normalize_adjust_mode(text).value
    except ValueError as exc:
        raise ValueError("adjust must be one of: none, qfq, hfq") from exc


def _blank_to_none(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "none":
            return None
        return text
    return value


def _normalize_choice(name: str, value: str, choices: tuple[str, ...]) -> str:
    text = str(value).strip().lower()
    if text not in choices:
        raise ValueError(f"{name} must be one of: {', '.join(choices)}")
    return text


def _slice_items(items: list[Any], *, start: int = 0, limit: int | None = None) -> list[Any]:
    if start < 0:
        raise ValueError("start must be >= 0")
    if limit is not None and limit < 0:
        raise ValueError("limit must be >= 0")
    if limit == 0:
        return []
    end = None if limit is None else start + limit
    return items[start:end]


def _limited_page(payload: dict[str, Any], *, start: int = 0, limit: int | None = 1000) -> dict[str, Any]:
    items = list(payload.get("items", []))
    total = len(items)
    sliced = _slice_items(items, start=start, limit=limit)
    result = dict(payload)
    result["start"] = start
    result["limit"] = limit
    result["total"] = total
    result["count"] = len(sliced)
    result["items"] = sliced
    return result


def _client_kwargs(host: str | None, timeout: float, pool_size: int, probe_hosts: bool) -> dict[str, Any]:
    return {"host": host, "timeout": timeout, "pool_size": pool_size, "probe_hosts": probe_hosts}


def _page_payload(response: Any, **meta: Any) -> dict[str, Any]:
    payload = to_jsonable(response)
    result = dict(meta)
    for key in ("trading_date", "count", "items", "raw_frame_hex", "raw_payload_hex"):
        if key in payload:
            result[key] = payload[key]
    return result


def _code_page_payload(response: Any) -> dict[str, Any]:
    payload = to_jsonable(response)
    for item in payload.get("items", []):
        if "full_code" not in item and item.get("exchange") and item.get("code"):
            item["full_code"] = f"{item['exchange']}{item['code']}"
    return payload


def get_kline_data(
    code: str,
    period: str = "day",
    *,
    start: int = 0,
    count: int = 200,
    kind: str = "stock",
    adjust: str | None = None,
    include_raw: bool = False,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    normalized_adjust = _normalize_adjust(adjust)
    if normalized_adjust is not None and kind != "stock":
        raise ValueError("adjusted kline only supports kind='stock'")

    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        if normalized_adjust is None:
            response = client.get_kline(code, period, start=start, count=count, kind=kind, include_raw=include_raw)
        else:
            response = client.get_adjusted_kline(period, code, adjust=normalized_adjust, start=start, count=count, include_raw=include_raw)

    payload = to_jsonable(response)
    return {
        "code": code,
        "period": period,
        "kind": kind,
        "adjust": normalized_adjust,
        "start": start,
        "request_count": count,
        "count": payload["count"],
        "items": payload["items"],
        "raw_frame_hex": payload.get("raw_frame_hex"),
        "raw_payload_hex": payload.get("raw_payload_hex"),
    }


def get_kline_all_data(
    code: str,
    period: str = "day",
    *,
    kind: str = "stock",
    adjust: str | None = None,
    start: int = 0,
    limit: int | None = 1000,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    normalized_adjust = _normalize_adjust(adjust)
    if normalized_adjust is not None and kind != "stock":
        raise ValueError("adjusted kline only supports kind='stock'")

    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        if normalized_adjust is None:
            response = client.get_kline_all(code, period, kind=kind)
        else:
            response = client.get_adjusted_kline_all(period, code, adjust=normalized_adjust)

    payload = _limited_page(to_jsonable(response), start=start, limit=limit)
    return {
        "code": code,
        "period": period,
        "kind": kind,
        "adjust": normalized_adjust,
        "start": payload["start"],
        "limit": payload["limit"],
        "total": payload["total"],
        "count": payload["count"],
        "items": payload["items"],
    }


def get_quote_data(
    codes: str | list[str] | tuple[str, ...],
    *,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 2,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    code_list = _normalize_codes(codes)
    if not code_list:
        raise ValueError("at least one code is required")

    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        quotes = client.get_quote(code_list)

    payload = to_jsonable(quotes)
    return {
        "codes": code_list,
        "request_count": len(code_list),
        "count": len(payload),
        "quotes": payload,
    }


def get_minute_data(
    code: str,
    date: Any = None,
    *,
    include_raw: bool = False,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    date_value = _blank_to_none(date)
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        response = client.get_minute(code, date_value, include_raw=include_raw)
    return _page_payload(response, code=code, date=to_jsonable(date_value))


def get_trades_data(
    code: str,
    date: Any = None,
    *,
    start: int = 0,
    count: int = 200,
    include_raw: bool = False,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    date_value = _blank_to_none(date)
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        response = client.get_trades(code, date_value, start=start, count=count, include_raw=include_raw)
    return _page_payload(response, code=code, date=to_jsonable(date_value), start=start, request_count=count)


def get_trades_all_data(
    code: str,
    date: Any = None,
    *,
    start: int = 0,
    limit: int | None = 1000,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    date_value = _blank_to_none(date)
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        response = client.get_trades_all(code, date_value)
    payload = _limited_page(to_jsonable(response), start=start, limit=limit)
    return {
        "code": code,
        "date": to_jsonable(date_value),
        "trading_date": payload.get("trading_date"),
        "start": payload["start"],
        "limit": payload["limit"],
        "total": payload["total"],
        "count": payload["count"],
        "items": payload["items"],
    }


def get_trade_minute_kline_data(
    code: str,
    date: Any = None,
    *,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    date_value = _blank_to_none(date)
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        if date_value is None:
            response = client.get_trade_minute_kline(code)
        else:
            response = client.get_history_trade_minute_kline(code, date_value)

    payload = to_jsonable(response)
    return {"code": code, "date": to_jsonable(date_value), "count": payload["count"], "items": payload["items"]}


def get_auction_0925_data(
    code: str,
    date: Any,
    *,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    date_value = _blank_to_none(date)
    if date_value is None:
        raise ValueError("date is required")

    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        response = client.get_auction_0925(code, date_value)

    payload = to_jsonable(response)
    return {"request_code": code, "date": to_jsonable(date_value), **payload}


def get_call_auction_data(
    code: str,
    *,
    include_raw: bool = False,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        response = client.get_call_auction(code, include_raw=include_raw)
    return _page_payload(response, code=code)


def get_count_data(
    exchange: str,
    *,
    kind: str = "code",
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    count_kind = _normalize_choice("kind", kind, ("code", "stock", "a_share"))
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        if count_kind == "stock":
            count = client.get_stock_count(exchange)
        elif count_kind == "a_share":
            count = client.get_a_share_count(exchange)
        else:
            count = client.get_count(exchange)
    return {"exchange": exchange, "kind": count_kind, "count": count}


def get_codes_data(
    exchange: str,
    *,
    start: int = 0,
    limit: int | None = 1000,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        response = client.get_codes(exchange, start=start, limit=limit)
    return _code_page_payload(response)


def get_code_list_data(
    *,
    kind: str = "a_share",
    start: int = 0,
    limit: int | None = 1000,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    list_kind = _normalize_choice("kind", kind, ("a_share", "stock", "etf", "index"))
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        if list_kind == "stock":
            codes = client.get_stock_codes_all()
        elif list_kind == "etf":
            codes = client.get_etf_codes_all()
        elif list_kind == "index":
            codes = client.get_index_codes_all()
        else:
            codes = client.get_a_share_codes_all()

    items = _slice_items(list(codes), start=start, limit=limit)
    return {"kind": list_kind, "start": start, "limit": limit, "total": len(codes), "count": len(items), "codes": items}


def get_gbbq_data(
    code: str,
    *,
    include_raw: bool = False,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        response = client.get_gbbq(code, include_raw=include_raw)
    return _page_payload(response, code=code)


def get_xdxr_data(
    code: str,
    *,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        items = client.get_xdxr(code)
    payload = to_jsonable(items)
    return {"code": code, "count": len(payload), "items": payload}


def get_equity_changes_data(
    code: str,
    *,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        response = client.get_equity_changes(code)
    return _page_payload(response, code=code)


def get_equity_data(
    code: str,
    on: Any = None,
    *,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    on_value = _blank_to_none(on)
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        equity = client.get_equity(code, on_value)
    payload = to_jsonable(equity)
    return {"code": code, "on": to_jsonable(on_value), "found": payload is not None, "equity": payload}


def get_turnover_data(
    code: str,
    volume: int | float,
    *,
    on: Any = None,
    unit: str = "hand",
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    on_value = _blank_to_none(on)
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        turnover = client.get_turnover(code, volume, on=on_value, unit=unit)
    return {"code": code, "volume": volume, "unit": unit, "on": to_jsonable(on_value), "turnover": turnover}


def get_factors_data(
    code: str,
    *,
    start: int = 0,
    limit: int | None = 1000,
    host: str | None = None,
    timeout: float = 8.0,
    pool_size: int = 1,
    probe_hosts: bool = False,
) -> dict[str, Any]:
    with TdxClient(**_client_kwargs(host, timeout, pool_size, probe_hosts)) as client:
        response = client.get_factors(code)
    payload = _limited_page(to_jsonable(response), start=start, limit=limit)
    return {
        "code": code,
        "start": payload["start"],
        "limit": payload["limit"],
        "total": payload["total"],
        "count": payload["count"],
        "items": payload["items"],
    }
