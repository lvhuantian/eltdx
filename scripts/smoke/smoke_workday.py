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
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    with TdxClient(timeout=6, heartbeat_interval=None) as client:
        service = client.workdays
        print(f"[OK] loaded={service.refresh()} benchmark_days")
        print(f"[OK] date={service.normalize(args.date)} is_workday={service.is_workday(args.date)}")
        print(f"[OK] previous_known={service.previous_workday(args.date)} next_known={service.next_workday(args.date)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
