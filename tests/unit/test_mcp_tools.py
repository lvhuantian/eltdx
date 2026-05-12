from __future__ import annotations

from datetime import date, datetime

import pytest

from eltdx.mcp_tools import (
    get_auction_0925_data,
    get_call_auction_data,
    get_code_list_data,
    get_codes_data,
    get_count_data,
    get_equity_changes_data,
    get_equity_data,
    get_factors_data,
    get_gbbq_data,
    get_kline_all_data,
    get_kline_data,
    get_minute_data,
    get_quote_data,
    get_trade_minute_kline_data,
    get_trades_all_data,
    get_trades_data,
    get_turnover_data,
    get_xdxr_data,
)
from eltdx.models import KlineItem, KlineResponse, Quote
from eltdx.protocol.unit import SHANGHAI_TZ


def _make_response(close_price_milli: int = 11280) -> KlineResponse:
    return KlineResponse(
        count=1,
        items=[
            KlineItem(
                time=datetime(2026, 5, 11, 15, 0, tzinfo=SHANGHAI_TZ),
                open_price=11.2,
                open_price_milli=11200,
                high_price=11.3,
                high_price_milli=11300,
                low_price=11.1,
                low_price_milli=11100,
                close_price=close_price_milli / 1000,
                close_price_milli=close_price_milli,
                last_close_price=11.2,
                last_close_price_milli=11200,
                volume=100,
                amount=112800.0,
                amount_milli=112800000,
            )
        ],
    )


class _FakeClient:
    instances: list[_FakeClient] = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.calls: list[tuple] = []
        self.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def get_kline(self, *args, **kwargs):
        self.calls.append(("get_kline", args, kwargs))
        return _make_response()

    def get_adjusted_kline(self, *args, **kwargs):
        self.calls.append(("get_adjusted_kline", args, kwargs))
        return _make_response(11330)

    def get_kline_all(self, *args, **kwargs):
        self.calls.append(("get_kline_all", args, kwargs))
        response = _make_response()
        response.items = [response.items[0], _make_response(11330).items[0]]
        response.count = 2
        return response

    def get_adjusted_kline_all(self, *args, **kwargs):
        self.calls.append(("get_adjusted_kline_all", args, kwargs))
        response = _make_response(11330)
        response.items = [response.items[0], _make_response(11440).items[0]]
        response.count = 2
        return response

    def get_quote(self, *args, **kwargs):
        self.calls.append(("get_quote", args, kwargs))
        return [
            Quote(
                exchange="sz",
                code="000001",
                active1=0,
                active2=0,
                server_time_raw=15331973,
                server_time=datetime(2026, 5, 12, 15, 33, 19, 730000, tzinfo=SHANGHAI_TZ),
                last_price=11.28,
                last_price_milli=11280,
                open_price=11.2,
                open_price_milli=11200,
                high_price=11.3,
                high_price_milli=11300,
                low_price=11.1,
                low_price_milli=11100,
                last_close_price=11.2,
                last_close_price_milli=11200,
                total_hand=1000,
                current_hand=10,
                amount=100000.0,
                inside_dish=1,
                outer_disc=2,
                buy_levels=[],
                sell_levels=[],
                rate=0.7,
            )
        ]

    def get_minute(self, *args, **kwargs):
        self.calls.append(("get_minute", args, kwargs))
        return {
            "count": 1,
            "trading_date": date(2026, 5, 12),
            "items": [{"time": datetime(2026, 5, 12, 9, 31, tzinfo=SHANGHAI_TZ), "price": 11.28}],
            "raw_frame_hex": "aa",
            "raw_payload_hex": "bb",
        }

    def get_trades(self, *args, **kwargs):
        self.calls.append(("get_trades", args, kwargs))
        return {
            "count": 1,
            "trading_date": date(2026, 5, 12),
            "items": [{"time": datetime(2026, 5, 12, 9, 31, tzinfo=SHANGHAI_TZ), "price": 11.28, "volume": 10}],
        }

    def get_trades_all(self, *args, **kwargs):
        self.calls.append(("get_trades_all", args, kwargs))
        return {"count": 3, "trading_date": date(2026, 5, 12), "items": [{"volume": 10}, {"volume": 20}, {"volume": 30}]}

    def get_trade_minute_kline(self, *args, **kwargs):
        self.calls.append(("get_trade_minute_kline", args, kwargs))
        return _make_response()

    def get_history_trade_minute_kline(self, *args, **kwargs):
        self.calls.append(("get_history_trade_minute_kline", args, kwargs))
        return _make_response(11330)

    def get_auction_0925(self, *args, **kwargs):
        self.calls.append(("get_auction_0925", args, kwargs))
        return {
            "code": "sz000001",
            "trading_date": date(2026, 5, 12),
            "has_auction_0925": True,
            "price": 11.28,
            "volume": 100,
        }

    def get_call_auction(self, *args, **kwargs):
        self.calls.append(("get_call_auction", args, kwargs))
        return {"count": 1, "items": [{"price": 11.28, "match": 100}], "raw_frame_hex": "aa"}

    def get_count(self, *args, **kwargs):
        self.calls.append(("get_count", args, kwargs))
        return 3000

    def get_stock_count(self, *args, **kwargs):
        self.calls.append(("get_stock_count", args, kwargs))
        return 3100

    def get_a_share_count(self, *args, **kwargs):
        self.calls.append(("get_a_share_count", args, kwargs))
        return 2800

    def get_codes(self, *args, **kwargs):
        self.calls.append(("get_codes", args, kwargs))
        return {
            "exchange": args[0],
            "start": kwargs["start"],
            "count": 1,
            "total": 2,
            "items": [{"exchange": "sz", "code": "000001", "name": "平安银行", "full_code": "sz000001"}],
        }

    def get_a_share_codes_all(self, *args, **kwargs):
        self.calls.append(("get_a_share_codes_all", args, kwargs))
        return ["sz000001", "sh600000", "bj920001", "sz000002"]

    def get_stock_codes_all(self, *args, **kwargs):
        self.calls.append(("get_stock_codes_all", args, kwargs))
        return ["sz000001", "sh600000"]

    def get_etf_codes_all(self, *args, **kwargs):
        self.calls.append(("get_etf_codes_all", args, kwargs))
        return ["sh510300"]

    def get_index_codes_all(self, *args, **kwargs):
        self.calls.append(("get_index_codes_all", args, kwargs))
        return ["sh000001"]

    def get_gbbq(self, *args, **kwargs):
        self.calls.append(("get_gbbq", args, kwargs))
        return {"count": 1, "items": [{"code": "000001", "category_name": "股本变化"}], "raw_payload_hex": "bb"}

    def get_xdxr(self, *args, **kwargs):
        self.calls.append(("get_xdxr", args, kwargs))
        return [{"code": "000001", "fenhong": 1.0}]

    def get_equity_changes(self, *args, **kwargs):
        self.calls.append(("get_equity_changes", args, kwargs))
        return {"count": 1, "items": [{"code": "000001", "float_shares": 1000}]}

    def get_equity(self, *args, **kwargs):
        self.calls.append(("get_equity", args, kwargs))
        return {"code": "000001", "float_shares": 1000}

    def get_turnover(self, *args, **kwargs):
        self.calls.append(("get_turnover", args, kwargs))
        return 12.34

    def get_factors(self, *args, **kwargs):
        self.calls.append(("get_factors", args, kwargs))
        return {"count": 3, "items": [{"qfq_factor": 1.0}, {"qfq_factor": 1.1}, {"qfq_factor": 1.2}]}


@pytest.fixture(autouse=True)
def reset_fake_client() -> None:
    _FakeClient.instances = []


def test_get_kline_data_returns_jsonable_payload(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    parsed = get_kline_data("sz000001", "day", start=2, count=3, kind="stock", timeout=5.0, probe_hosts=True)

    assert parsed["code"] == "sz000001"
    assert parsed["period"] == "day"
    assert parsed["kind"] == "stock"
    assert parsed["adjust"] is None
    assert parsed["start"] == 2
    assert parsed["request_count"] == 3
    assert parsed["count"] == 1
    assert parsed["items"][0]["time"] == "2026-05-11T15:00:00+08:00"
    assert parsed["items"][0]["close_price_milli"] == 11280

    client = _FakeClient.instances[0]
    assert client.kwargs == {"host": None, "timeout": 5.0, "pool_size": 1, "probe_hosts": True}
    assert client.calls == [
        ("get_kline", ("sz000001", "day"), {"start": 2, "count": 3, "kind": "stock", "include_raw": False})
    ]


def test_get_kline_data_uses_adjusted_kline(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    parsed = get_kline_data("sz000001", "day", adjust=" QFQ ", count=5)

    assert parsed["adjust"] == "qfq"
    assert parsed["items"][0]["close_price_milli"] == 11330
    assert _FakeClient.instances[0].calls == [
        ("get_adjusted_kline", ("day", "sz000001"), {"adjust": "qfq", "start": 0, "count": 5, "include_raw": False})
    ]


def test_get_kline_all_data(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    parsed = get_kline_all_data("sz000001", "day", adjust="hfq", start=1, limit=1)

    assert parsed["adjust"] == "hfq"
    assert parsed["count"] == 1
    assert parsed["total"] == 2
    assert parsed["items"][0]["close_price_milli"] == 11440
    assert _FakeClient.instances[0].calls == [("get_adjusted_kline_all", ("day", "sz000001"), {"adjust": "hfq"})]


def test_get_kline_data_rejects_invalid_adjust() -> None:
    with pytest.raises(ValueError, match="adjust must be"):
        get_kline_data("sz000001", "day", adjust="bad")


def test_get_kline_data_rejects_adjusted_index(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    with pytest.raises(ValueError, match="adjusted kline only supports"):
        get_kline_data("sh000001", "day", kind="index", adjust="qfq")

    assert _FakeClient.instances == []


def test_get_quote_data_returns_jsonable_payload(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    parsed = get_quote_data("sz000001, sh600000", timeout=5.0, pool_size=3, probe_hosts=True)

    assert parsed["codes"] == ["sz000001", "sh600000"]
    assert parsed["request_count"] == 2
    assert parsed["count"] == 1
    assert parsed["quotes"][0]["server_time"] == "2026-05-12T15:33:19.730000+08:00"
    assert parsed["quotes"][0]["last_price_milli"] == 11280

    client = _FakeClient.instances[0]
    assert client.kwargs == {"host": None, "timeout": 5.0, "pool_size": 3, "probe_hosts": True}
    assert client.calls == [("get_quote", (["sz000001", "sh600000"],), {})]


def test_get_quote_data_rejects_empty_codes() -> None:
    with pytest.raises(ValueError, match="at least one code"):
        get_quote_data(" , ")


def test_get_minute_and_trade_pages(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    minute = get_minute_data("sz000001", "2026-05-12", include_raw=True)
    trades = get_trades_data("sz000001", "2026-05-12", start=10, count=20)

    assert minute["trading_date"] == "2026-05-12"
    assert minute["items"][0]["time"] == "2026-05-12T09:31:00+08:00"
    assert trades["start"] == 10
    assert trades["request_count"] == 20
    assert _FakeClient.instances[0].calls == [
        ("get_minute", ("sz000001", "2026-05-12"), {"include_raw": True})
    ]
    assert _FakeClient.instances[1].calls == [
        ("get_trades", ("sz000001", "2026-05-12"), {"start": 10, "count": 20, "include_raw": False})
    ]


def test_get_trades_all_and_trade_minute_kline(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    trades = get_trades_all_data("sz000001", start=1, limit=1)
    kline = get_trade_minute_kline_data("sz000001", "2026-05-12")

    assert trades["total"] == 3
    assert trades["count"] == 1
    assert trades["items"] == [{"volume": 20}]
    assert kline["items"][0]["close_price_milli"] == 11330
    assert _FakeClient.instances[0].calls == [("get_trades_all", ("sz000001", None), {})]
    assert _FakeClient.instances[1].calls == [("get_history_trade_minute_kline", ("sz000001", "2026-05-12"), {})]


def test_get_auction_data(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    auction_0925 = get_auction_0925_data("000001", "2026-05-12")
    call_auction = get_call_auction_data("sz000001", include_raw=True)

    assert auction_0925["request_code"] == "000001"
    assert auction_0925["trading_date"] == "2026-05-12"
    assert call_auction["count"] == 1
    assert _FakeClient.instances[0].calls == [("get_auction_0925", ("000001", "2026-05-12"), {})]
    assert _FakeClient.instances[1].calls == [("get_call_auction", ("sz000001",), {"include_raw": True})]


def test_get_auction_0925_requires_date() -> None:
    with pytest.raises(ValueError, match="date is required"):
        get_auction_0925_data("000001", " ")


def test_get_code_tools(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    count = get_count_data("sz", kind="a_share")
    page = get_codes_data("sz", start=5, limit=10)
    codes = get_code_list_data(kind="a_share", start=1, limit=1)

    assert count == {"exchange": "sz", "kind": "a_share", "count": 2800}
    assert page["items"][0]["full_code"] == "sz000001"
    assert codes == {"kind": "a_share", "start": 1, "limit": 1, "total": 4, "count": 1, "codes": ["sh600000"]}
    assert _FakeClient.instances[0].calls == [("get_a_share_count", ("sz",), {})]
    assert _FakeClient.instances[1].calls == [("get_codes", ("sz",), {"start": 5, "limit": 10})]
    assert _FakeClient.instances[2].calls == [("get_a_share_codes_all", (), {})]


def test_get_code_tools_reject_invalid_kind() -> None:
    with pytest.raises(ValueError, match="kind must be"):
        get_count_data("sz", kind="bad")
    with pytest.raises(ValueError, match="kind must be"):
        get_code_list_data(kind="bad")


def test_get_corporate_action_tools(monkeypatch) -> None:
    monkeypatch.setattr("eltdx.mcp_tools.TdxClient", _FakeClient)

    gbbq = get_gbbq_data("sz000001", include_raw=True)
    xdxr = get_xdxr_data("sz000001")
    equity_changes = get_equity_changes_data("sz000001")
    equity = get_equity_data("sz000001", "2026-05-12")
    turnover = get_turnover_data("sz000001", 1000, on="2026-05-12", unit="hand")
    factors = get_factors_data("sz000001", start=1, limit=1)

    assert gbbq["raw_payload_hex"] == "bb"
    assert xdxr["count"] == 1
    assert equity_changes["items"][0]["float_shares"] == 1000
    assert equity["found"] is True
    assert turnover["turnover"] == 12.34
    assert factors["total"] == 3
    assert factors["items"] == [{"qfq_factor": 1.1}]
    assert _FakeClient.instances[0].calls == [("get_gbbq", ("sz000001",), {"include_raw": True})]
    assert _FakeClient.instances[1].calls == [("get_xdxr", ("sz000001",), {})]
    assert _FakeClient.instances[2].calls == [("get_equity_changes", ("sz000001",), {})]
    assert _FakeClient.instances[3].calls == [("get_equity", ("sz000001", "2026-05-12"), {})]
    assert _FakeClient.instances[4].calls == [("get_turnover", ("sz000001", 1000), {"on": "2026-05-12", "unit": "hand"})]
    assert _FakeClient.instances[5].calls == [("get_factors", ("sz000001",), {})]
