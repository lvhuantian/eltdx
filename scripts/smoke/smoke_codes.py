from __future__ import annotations

import sys
from pathlib import Path

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "pyproject.toml").exists())
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from eltdx import TdxClient


def main() -> int:
    with TdxClient(timeout=6, heartbeat_interval=None) as client:
        for market in ("sh", "sz", "bj"):
            count = client.get_count(market)
            page = client.get_codes(market, limit=3)
            print(f"[OK] {market} count={count} sample={[item.full_code for item in page]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
