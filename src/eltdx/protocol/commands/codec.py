"""Command-level builders and parsers for migrated 7709 commands."""

from __future__ import annotations

from typing import Any, Callable

from eltdx.exceptions import UnsupportedCommandError
from eltdx.protocol.constants import (
    TYPE_AUCTION_SERIES,
    TYPE_CAPITAL_CHANGES,
    TYPE_CATEGORY_QUOTES,
    TYPE_FINANCE_BATCH,
    TYPE_HANDSHAKE,
    TYPE_HEARTBEAT,
    TYPE_HISTORICAL_INTRADAY,
    TYPE_HISTORICAL_TICKS,
    TYPE_INTRADAY_AUX,
    TYPE_KLINES,
    TYPE_REFRESH_STREAM,
    TYPE_RECENT_INTRADAY,
    TYPE_SECURITY_COUNT,
    TYPE_SECURITY_LIST,
    TYPE_SNAPSHOTS,
    TYPE_SPARKLINE,
    TYPE_SPECIAL_LIMITS,
    TYPE_TODAY_INTRADAY,
    TYPE_TODAY_TICKS,
)
from eltdx.protocol.frame import RequestFrame, ResponseFrame

from .auctions import build_auction_series_frame, parse_auction_series_payload
from .corporate import (
    build_capital_changes_frame,
    build_finance_batch_frame,
    parse_capital_changes_payload,
    parse_finance_batch_payload,
)
from .klines import build_klines_frame, parse_klines_payload
from .limits import build_special_limits_frame, parse_special_limits_payload
from .minutes import (
    build_historical_intraday_frame,
    build_intraday_aux_frame,
    build_recent_intraday_frame,
    build_sparkline_frame,
    build_today_intraday_frame,
    parse_historical_intraday_payload,
    parse_intraday_aux_payload,
    parse_recent_intraday_payload,
    parse_sparkline_payload,
    parse_today_intraday_payload,
)
from .quotes import (
    build_category_quotes_frame,
    build_refresh_stream_frame,
    build_snapshots_frame,
    parse_category_quotes_payload,
    parse_refresh_stream_payload,
    parse_snapshots_payload,
)
from .security import (
    build_security_count_frame,
    build_security_list_frame,
    parse_security_count_payload,
    parse_security_list_payload,
)
from .session import build_handshake_frame, build_heartbeat_frame, parse_handshake_payload, parse_heartbeat_payload
from .trades import (
    build_historical_ticks_frame,
    build_today_ticks_frame,
    parse_historical_ticks_payload,
    parse_today_ticks_payload,
)

Builder = Callable[[dict[str, Any], int], RequestFrame]
Parser = Callable[[ResponseFrame, dict[str, Any] | None], Any]


BUILDERS: dict[int, Builder] = {
    TYPE_AUCTION_SERIES: build_auction_series_frame,
    TYPE_CAPITAL_CHANGES: build_capital_changes_frame,
    TYPE_CATEGORY_QUOTES: build_category_quotes_frame,
    TYPE_FINANCE_BATCH: build_finance_batch_frame,
    TYPE_HANDSHAKE: build_handshake_frame,
    TYPE_HEARTBEAT: build_heartbeat_frame,
    TYPE_HISTORICAL_INTRADAY: build_historical_intraday_frame,
    TYPE_HISTORICAL_TICKS: build_historical_ticks_frame,
    TYPE_INTRADAY_AUX: build_intraday_aux_frame,
    TYPE_KLINES: build_klines_frame,
    TYPE_REFRESH_STREAM: build_refresh_stream_frame,
    TYPE_RECENT_INTRADAY: build_recent_intraday_frame,
    TYPE_SECURITY_COUNT: build_security_count_frame,
    TYPE_SECURITY_LIST: build_security_list_frame,
    TYPE_SNAPSHOTS: build_snapshots_frame,
    TYPE_SPARKLINE: build_sparkline_frame,
    TYPE_SPECIAL_LIMITS: build_special_limits_frame,
    TYPE_TODAY_INTRADAY: build_today_intraday_frame,
    TYPE_TODAY_TICKS: build_today_ticks_frame,
}


def _parse_handshake(response: ResponseFrame, request_payload: dict[str, Any] | None = None) -> Any:
    return parse_handshake_payload(response)


def _parse_heartbeat(response: ResponseFrame, request_payload: dict[str, Any] | None = None) -> Any:
    return parse_heartbeat_payload(response)


def _parse_security_count(response: ResponseFrame, request_payload: dict[str, Any] | None = None) -> Any:
    return parse_security_count_payload(response)


PARSERS: dict[int, Parser] = {
    TYPE_AUCTION_SERIES: parse_auction_series_payload,
    TYPE_CAPITAL_CHANGES: parse_capital_changes_payload,
    TYPE_CATEGORY_QUOTES: parse_category_quotes_payload,
    TYPE_FINANCE_BATCH: parse_finance_batch_payload,
    TYPE_HANDSHAKE: _parse_handshake,
    TYPE_HEARTBEAT: _parse_heartbeat,
    TYPE_HISTORICAL_INTRADAY: parse_historical_intraday_payload,
    TYPE_HISTORICAL_TICKS: parse_historical_ticks_payload,
    TYPE_INTRADAY_AUX: parse_intraday_aux_payload,
    TYPE_KLINES: parse_klines_payload,
    TYPE_REFRESH_STREAM: parse_refresh_stream_payload,
    TYPE_RECENT_INTRADAY: parse_recent_intraday_payload,
    TYPE_SECURITY_COUNT: _parse_security_count,
    TYPE_SECURITY_LIST: parse_security_list_payload,
    TYPE_SNAPSHOTS: parse_snapshots_payload,
    TYPE_SPARKLINE: parse_sparkline_payload,
    TYPE_SPECIAL_LIMITS: parse_special_limits_payload,
    TYPE_TODAY_INTRADAY: parse_today_intraday_payload,
    TYPE_TODAY_TICKS: parse_today_ticks_payload,
}


def build_command_frame(command: int, payload: dict[str, Any] | None, msg_id: int) -> RequestFrame:
    try:
        builder = BUILDERS[command]
    except KeyError as exc:
        raise UnsupportedCommandError(f"7709 command 0x{command:04x} is not migrated yet") from exc
    return builder(dict(payload or {}), msg_id)


def parse_command_response(command: int, response: ResponseFrame, request_payload: dict[str, Any] | None = None) -> Any:
    try:
        parser = PARSERS[command]
    except KeyError as exc:
        raise UnsupportedCommandError(f"7709 command 0x{command:04x} is not migrated yet") from exc
    return parser(response, request_payload)
