from datetime import date

from eltdx import TdxClient
from eltdx.mcp import docs_index, kline, quote
from eltdx.models import KlineBar, KlineSeries, QuoteSnapshot


def test_mcp_docs_index_lists_main_documents() -> None:
    index = docs_index()

    assert index["API"] == "docs/API_REFERENCE.md"
    assert index["helpers"] == "docs/helpers/README.md"


def test_mcp_quote_returns_jsonable_snapshot(monkeypatch) -> None:
    snapshot = QuoteSnapshot(
        exchange="sz",
        market_id=0,
        code="000001",
        active1=0,
        last_price=12.0,
        pre_close_price=10.0,
        open_price=11.0,
        high_price=12.5,
        low_price=10.8,
        time_raw=0,
        unknown_after_time_raw=0,
        total_hand=5000,
        current_hand=100,
        amount=6_000_000.0,
        amount_raw=0,
        inside_dish=0,
        outer_disc=0,
        unknown_after_outer_raw=0,
        open_amount_raw=0,
        open_amount_yuan=1_000_000.0,
        buy_levels=(),
        sell_levels=(),
        tail_raw=b"",
    )

    monkeypatch.setattr(TdxClient, "connect", lambda self: None)
    monkeypatch.setattr(TdxClient, "close", lambda self: None)
    monkeypatch.setattr(TdxClient, "get_quote", lambda self, codes: [snapshot])

    result = quote("sz000001", timeout=1)

    assert result[0]["code"] == "000001"
    assert result[0]["last_price"] == 12.0
    assert result[0]["tail_raw"] == ""


def test_mcp_kline_returns_jsonable_series(monkeypatch) -> None:
    series = KlineSeries(
        exchange="sz",
        market_id=0,
        code="000001",
        period_raw=9,
        period_param_raw=9,
        period_name="day",
        start=0,
        request_count=1,
        adjust_mode_raw=0,
        adjust_mode="none",
        anchor_date_raw=0,
        anchor_date=date(2026, 5, 20),
        bars=(
            KlineBar(
                time=date(2026, 5, 20),
                open=10.0,
                close=11.0,
                high=11.5,
                low=9.8,
                open_price_milli=10000,
                close_price_milli=11000,
                high_price_milli=11500,
                low_price_milli=9800,
                last_close_price_milli=9900,
                volume_raw=1,
                amount_raw=2,
                volume_wire_value=100.0,
                volume_lots=100.0,
                amount=110000.0,
                open_delta_raw=0,
                close_delta_raw=0,
                high_delta_raw=0,
                low_delta_raw=0,
            ),
        ),
    )

    monkeypatch.setattr(TdxClient, "connect", lambda self: None)
    monkeypatch.setattr(TdxClient, "close", lambda self: None)
    monkeypatch.setattr(TdxClient, "get_kline", lambda self, *args, **kwargs: series)

    result = kline("sz000001", timeout=1)

    assert result["period_name"] == "day"
    assert result["anchor_date"] == "2026-05-20"
    assert result["bars"][0]["time"] == "2026-05-20"
