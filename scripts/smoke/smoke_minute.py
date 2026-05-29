from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "pyproject.toml").exists())
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from eltdx import TdxClient


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", default="sz000001")
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    with TdxClient(timeout=6, heartbeat_interval=None) as client:
        response = client.get_minute(args.code, args.date)
        print(f"[OK] minute code={response.full_code} date={response.trading_date} count={response.count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
