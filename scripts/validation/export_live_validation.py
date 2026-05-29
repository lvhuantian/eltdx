from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "pyproject.toml").exists())
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from eltdx import TdxClient


def main() -> int:
    parser = argparse.ArgumentParser(description="Export a small live validation CSV.")
    parser.add_argument("--output", default="output/live_validation.csv")
    parser.add_argument("--codes", nargs="+", default=["sz000001", "sh600000"])
    args = parser.parse_args()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    with TdxClient(timeout=6, heartbeat_interval=None) as client:
        quotes = client.get_quote(args.codes)
        rows = [
            {
                "code": item.full_code,
                "last_price": item.last_price,
                "pre_close_price": item.pre_close_price,
                "change_pct": item.change_pct,
                "volume_hand": item.total_hand,
                "amount": item.amount,
            }
            for item in quotes
        ]

    with output.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=["code", "last_price", "pre_close_price", "change_pct", "volume_hand", "amount"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] exported {len(rows)} rows -> {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
