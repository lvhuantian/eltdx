"""Security code table API."""

from __future__ import annotations

from eltdx.protocol.constants import DEFAULT_CODE_PAGE_SIZE

from .base import ApiBase


class CodeApi(ApiBase):
    def count(self, market: str):
        return self._execute("security_count", market=market)

    def list(self, market: str, *, start: int = 0, limit: int = 1600):
        return self._execute("security_list", market=market, start=start, limit=limit)

    def all(self, market: str, *, page_size: int = DEFAULT_CODE_PAGE_SIZE):
        start = 0
        items = []
        while True:
            page = self.list(market, start=start, limit=page_size)
            items.extend(page)
            if len(page) < page_size:
                return items
            start += page_size
