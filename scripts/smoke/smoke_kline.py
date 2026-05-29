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
    parser.add_argument("--period", default="day")
    parser.add_argument("--count", type=int, default=5)
    args = parser.parse_args()

    with TdxClient(timeout=6, heartbeat_interval=None) as client:
        base = client.get_kline(args.period, args.code, count=args.count)
        qfq = client.get_adjusted_kline(args.period, args.code, adjust="qfq", count=args.count)
        print(f"[OK] base count={base.count} latest={base.bars[-1].time if base.bars else None}")
        print(f"[OK] qfq count={qfq.count} latest={qfq.bars[-1].time if qfq.bars else None}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
