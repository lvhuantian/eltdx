from __future__ import annotations

from typing import Any

from .mcp_tools import (
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


def create_server():
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:
        raise RuntimeError("Install MCP support with: python -m pip install 'eltdx[mcp]'") from exc

    server = FastMCP("eltdx")

    @server.tool(name="tdx_get_kline")
    def tdx_get_kline(
        code: str,
        period: str = "day",
        start: int = 0,
        count: int = 200,
        kind: str = "stock",
        adjust: str | None = None,
        include_raw: bool = False,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch one page of K-line data."""
        return get_kline_data(
            code,
            period,
            start=start,
            count=count,
            kind=kind,
            adjust=adjust,
            include_raw=include_raw,
            host=host,
            timeout=timeout,
            probe_hosts=probe_hosts,
        )

    @server.tool(name="tdx_get_kline_all")
    def tdx_get_kline_all(
        code: str,
        period: str = "day",
        kind: str = "stock",
        adjust: str | None = None,
        start: int = 0,
        limit: int | None = 1000,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch all available K-line pages for a code."""
        return get_kline_all_data(
            code,
            period,
            kind=kind,
            adjust=adjust,
            start=start,
            limit=limit,
            host=host,
            timeout=timeout,
            probe_hosts=probe_hosts,
        )

    @server.tool(name="tdx_get_quote")
    def tdx_get_quote(
        codes: str,
        host: str | None = None,
        timeout: float = 8.0,
        pool_size: int = 2,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch realtime quote snapshots for one or more comma-separated codes."""
        return get_quote_data(codes, host=host, timeout=timeout, pool_size=pool_size, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_minute")
    def tdx_get_minute(
        code: str,
        date: str | None = None,
        include_raw: bool = False,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch realtime or historical minute series."""
        return get_minute_data(code, date, include_raw=include_raw, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_trades")
    def tdx_get_trades(
        code: str,
        date: str | None = None,
        start: int = 0,
        count: int = 200,
        include_raw: bool = False,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch one page of realtime or historical transaction ticks."""
        return get_trades_data(
            code,
            date,
            start=start,
            count=count,
            include_raw=include_raw,
            host=host,
            timeout=timeout,
            probe_hosts=probe_hosts,
        )

    @server.tool(name="tdx_get_trades_all")
    def tdx_get_trades_all(
        code: str,
        date: str | None = None,
        start: int = 0,
        limit: int | None = 1000,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch all realtime or historical transaction ticks for a code."""
        return get_trades_all_data(code, date, start=start, limit=limit, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_trade_minute_kline")
    def tdx_get_trade_minute_kline(
        code: str,
        date: str | None = None,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Build minute K-line data from transaction ticks."""
        return get_trade_minute_kline_data(code, date, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_auction_0925")
    def tdx_get_auction_0925(
        code: str,
        date: str,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Find the 09:25 auction tick from historical transaction data."""
        return get_auction_0925_data(code, date, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_call_auction")
    def tdx_get_call_auction(
        code: str,
        include_raw: bool = False,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch realtime call-auction sequence."""
        return get_call_auction_data(code, include_raw=include_raw, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_count")
    def tdx_get_count(
        exchange: str,
        kind: str = "code",
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch code, stock, or A-share count for one exchange."""
        return get_count_data(exchange, kind=kind, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_codes")
    def tdx_get_codes(
        exchange: str,
        start: int = 0,
        limit: int | None = 1000,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch one page of security codes for an exchange."""
        return get_codes_data(exchange, start=start, limit=limit, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_code_list")
    def tdx_get_code_list(
        kind: str = "a_share",
        start: int = 0,
        limit: int | None = 1000,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch filtered code lists: a_share, stock, etf, or index."""
        return get_code_list_data(kind=kind, start=start, limit=limit, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_gbbq")
    def tdx_get_gbbq(
        code: str,
        include_raw: bool = False,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch raw share-capital change records."""
        return get_gbbq_data(code, include_raw=include_raw, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_xdxr")
    def tdx_get_xdxr(
        code: str,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch ex-right/ex-dividend records derived from GBBQ."""
        return get_xdxr_data(code, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_equity_changes")
    def tdx_get_equity_changes(
        code: str,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch float-share and total-share change records."""
        return get_equity_changes_data(code, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_equity")
    def tdx_get_equity(
        code: str,
        on: str | None = None,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Fetch the effective equity record for a code and date."""
        return get_equity_data(code, on, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_turnover")
    def tdx_get_turnover(
        code: str,
        volume: float,
        on: str | None = None,
        unit: str = "hand",
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Compute turnover rate from volume and effective float shares."""
        return get_turnover_data(code, volume, on=on, unit=unit, host=host, timeout=timeout, probe_hosts=probe_hosts)

    @server.tool(name="tdx_get_factors")
    def tdx_get_factors(
        code: str,
        start: int = 0,
        limit: int | None = 1000,
        host: str | None = None,
        timeout: float = 8.0,
        probe_hosts: bool = False,
    ) -> dict[str, Any]:
        """Build forward/backward adjustment factor series."""
        return get_factors_data(code, start=start, limit=limit, host=host, timeout=timeout, probe_hosts=probe_hosts)

    return server


def main() -> None:
    create_server().run()


if __name__ == "__main__":
    main()
