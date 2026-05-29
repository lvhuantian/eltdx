"""Intraday minute-chart API."""

from __future__ import annotations

from .base import ApiBase


class MinuteApi(ApiBase):
    def today(self, code: str, *, include_raw: bool = False):
        return self._execute("today_intraday", code=code, include_raw=include_raw)

    def history(self, code: str, trading_date, *, include_raw: bool = False):
        return self._execute("historical_intraday", code=code, trading_date=trading_date, include_raw=include_raw)

    def recent(self, code: str, trading_date=None, *, include_raw: bool = False):
        return self._execute("recent_intraday", code=code, trading_date=trading_date, include_raw=include_raw)

    def aux(self, code: str, kind: str | int = "buy_sell_strength", *, include_raw: bool = False):
        return self._execute("intraday_aux", code=code, kind=kind, include_raw=include_raw)

    def sparkline(self, code: str, *, selector: int = 1, window: int = 20, include_raw: bool = False):
        return self._execute("sparkline", code=code, selector=selector, window=window, include_raw=include_raw)
