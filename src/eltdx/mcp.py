"""MCP server entry for eltdx."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .client import TdxClient
from .serialization import to_jsonable


def quote(codes: str | Sequence[str], *, timeout: float = 8.0, host: str | None = None) -> Any:
    """Query quote snapshots."""

    with _client(timeout=timeout, host=host) as client:
        return _json(client.get_quote(codes))


def kline(
    code: str,
    *,
    period: str = "day",
    count: int = 120,
    start: int = 0,
    adjust: str | None = None,
    anchor_date: str | int | None = None,
    timeout: float = 8.0,
    host: str | None = None,
) -> Any:
    """Query K-line data."""

    with _client(timeout=timeout, host=host) as client:
        return _json(
            client.get_kline(
                period,
                code,
                start=start,
                count=count,
                adjust=adjust,
                anchor_date=anchor_date,
            )
        )


def stock_profile(codes: str | Sequence[str], *, timeout: float = 8.0, host: str | None = None) -> Any:
    """Return quote, code-table and finance fields in one table."""

    with _client(timeout=timeout, host=host) as client:
        return _json(client.helpers.stock_profile_table(codes))


def stock_topics(code: str, *, timeout: float = 8.0) -> Any:
    """Query all known topics for one stock."""

    client = TdxClient(timeout=timeout, heartbeat_interval=None)
    return _json(client.helpers.stock_topics(code))


def topic_stocks(
    seed_code: str,
    *,
    topic_id: str | int | None = None,
    topic_name: str | None = None,
    sort_by: str = "zdf",
    timeout: float = 8.0,
) -> Any:
    """Query stocks inside one topic."""

    client = TdxClient(timeout=timeout, heartbeat_interval=None)
    return _json(client.helpers.topic_stocks(seed_code, topic_id=topic_id, topic_name=topic_name, sort_by=sort_by))


def company_profile(code: str, *, timeout: float = 8.0) -> Any:
    """Query F10 company profile."""

    client = TdxClient(timeout=timeout, heartbeat_interval=None)
    return _json(client.f10.company_profile(code))


def hot_topics(code: str, *, timeout: float = 8.0) -> Any:
    """Query F10 hot-topic detail rows."""

    client = TdxClient(timeout=timeout, heartbeat_interval=None)
    return _json(client.f10.hot_topics(code))


def auction_0925(
    code: str,
    trading_date,
    *,
    timeout: float = 8.0,
    host: str | None = None,
    max_pages: int | None = 100,
) -> Any:
    """Query the 09:25 auction final tick from historical trade details."""

    with _client(timeout=timeout, host=host) as client:
        return _json(client.get_auction_0925(code, trading_date, max_pages=max_pages))


def docs_index() -> dict[str, str]:
    """Return local documentation entry points."""

    return {
        "README": "README.md",
        "API": "docs/API_REFERENCE.md",
        "methods": "docs/METHOD_REFERENCE.md",
        "fields": "docs/FIELD_REFERENCE.md",
        "7709_commands": "docs/COMMANDS_7709.md",
        "F10": "docs/F10_7615.md",
        "helpers": "docs/helpers/README.md",
    }


def create_mcp_server():
    """Create the FastMCP server."""

    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover - depends on optional package install
        raise RuntimeError("MCP support requires the 'mcp' package. Install eltdx with its dependencies.") from exc

    server = FastMCP("eltdx", instructions="eltdx A-share quote, K-line, F10 and topic data tools.")
    server.tool(name="eltdx_quote")(quote)
    server.tool(name="eltdx_kline")(kline)
    server.tool(name="eltdx_stock_profile")(stock_profile)
    server.tool(name="eltdx_stock_topics")(stock_topics)
    server.tool(name="eltdx_topic_stocks")(topic_stocks)
    server.tool(name="eltdx_company_profile")(company_profile)
    server.tool(name="eltdx_hot_topics")(hot_topics)
    server.tool(name="eltdx_auction_0925")(auction_0925)
    server.tool(name="eltdx_docs_index")(docs_index)
    return server


def main() -> int:
    """Run the MCP server over stdio."""

    create_mcp_server().run("stdio")
    return 0


def _client(*, timeout: float, host: str | None) -> TdxClient:
    return TdxClient(host=host, timeout=timeout, heartbeat_interval=None)


def _json(value: Any) -> Any:
    return to_jsonable(value)


if __name__ == "__main__":
    raise SystemExit(main())
