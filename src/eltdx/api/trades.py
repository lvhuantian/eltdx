"""Tick/trade API."""

from __future__ import annotations

from .base import ApiBase


class TradeApi(ApiBase):
    def today(self, code: str, *, start: int = 0, count: int = 1800, include_raw: bool = False):
        return self._execute("today_ticks", code=code, start=start, count=count, include_raw=include_raw)

    def history(self, code: str, trading_date, *, start: int = 0, count: int = 2000, include_raw: bool = False):
        return self._execute(
            "historical_ticks",
            code=code,
            trading_date=trading_date,
            start=start,
            count=count,
            include_raw=include_raw,
        )
