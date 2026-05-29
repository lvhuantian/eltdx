"""Auction API."""

from __future__ import annotations

from .base import ApiBase


class AuctionApi(ApiBase):
    def series(self, code: str, *, include_raw: bool = False):
        return self._execute("auction_series", code=code, include_raw=include_raw)
