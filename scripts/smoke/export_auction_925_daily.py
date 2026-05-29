from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "pyproject.toml").exists())
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from eltdx import TdxClient
from eltdx.protocol.unit import date_from_yyyymmdd, normalize_code, yyyymmdd


FIELDS = [
    "date",
    "code",
    "is_workday",
    "has_auction_0925",
    "price",
    "price_milli",
    "volume",
    "amount",
    "status",
    "side",
    "pages_used",
    "source_mode",
    "error",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export daily 09:25 auction ticks to CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--code", default="sz000001", help="stock code, with or without market prefix")
    parser.add_argument("--start", required=True, help="start date, for example 2026-04-01")
    parser.add_argument("--end", required=True, help="end date, for example 2026-04-30")
    parser.add_argument("--output", default=None, help="output CSV path")
    parser.add_argument("--export-dir", default="output/auction_0925", help="output directory used when --output is omitted")
    parser.add_argument("--host", default=None, help="single 7709 host")
    parser.add_argument("--timeout", type=float, default=8.0, help="socket timeout seconds")
    parser.add_argument("--page-size", type=int, default=2000, help="history trade page size")
    parser.add_argument("--max-pages", type=int, default=100, help="max history trade pages per day")
    parser.add_argument("--include-non-workday", action="store_true", help="also query dates not present in benchmark trading days")
    args = parser.parse_args()

    code = normalize_code(args.code)
    start_day = _parse_date(args.start)
    end_day = _parse_date(args.end)
    if start_day > end_day:
        start_day, end_day = end_day, start_day

    output = Path(args.output) if args.output else Path(args.export_dir) / f"{code}_{start_day:%Y%m%d}_{end_day:%Y%m%d}.csv"
    output.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, object]] = []
    with TdxClient(host=args.host, timeout=args.timeout, heartbeat_interval=None) as client:
        workdays = set(client.workdays.range(start_day, end_day))
        for trading_day in _date_range(start_day, end_day):
            is_workday = trading_day in workdays
            if not is_workday and not args.include_non_workday:
                rows.append(_blank_row(code, trading_day, is_workday=is_workday))
                continue

            try:
                result = client.get_auction_0925(
                    code,
                    trading_day,
                    page_size=args.page_size,
                    max_pages=args.max_pages,
                )
            except Exception as exc:
                rows.append(_blank_row(code, trading_day, is_workday=is_workday, error=type(exc).__name__))
                print(f"[WARN] {trading_day} failed: {type(exc).__name__}", flush=True)
                continue

            rows.append(
                {
                    "date": trading_day.isoformat(),
                    "code": result.code,
                    "is_workday": is_workday,
                    "has_auction_0925": result.has_auction_0925,
                    "price": result.price,
                    "price_milli": result.price_milli,
                    "volume": result.volume,
                    "amount": result.amount,
                    "status": result.status,
                    "side": result.side,
                    "pages_used": result.pages_used,
                    "source_mode": result.source_mode,
                    "error": "",
                }
            )
            print(f"[OK] {trading_day} has_auction_0925={result.has_auction_0925}", flush=True)

    with output.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] exported {len(rows)} rows -> {output}", flush=True)
    return 0


def _parse_date(value: str) -> date:
    parsed = date_from_yyyymmdd(yyyymmdd(value))
    if parsed is None:
        raise ValueError(f"invalid date: {value!r}")
    return parsed


def _date_range(start_day: date, end_day: date):
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def _blank_row(code: str, trading_day: date, *, is_workday: bool, error: str = "") -> dict[str, object]:
    return {
        "date": trading_day.isoformat(),
        "code": code,
        "is_workday": is_workday,
        "has_auction_0925": False,
        "price": "",
        "price_milli": "",
        "volume": "",
        "amount": "",
        "status": "",
        "side": "",
        "pages_used": "",
        "source_mode": "skipped_non_workday" if not is_workday and not error else "",
        "error": error,
    }


if __name__ == "__main__":
    raise SystemExit(main())
