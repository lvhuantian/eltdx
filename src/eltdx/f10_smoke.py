"""Live smoke command for the 7615 TQLEX / F10 gateway."""

from __future__ import annotations

import argparse

from eltdx.f10 import F10Client


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a live smoke check against the 7615 TQLEX / F10 gateway.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--code", default="000034", help="stock code to test")
    parser.add_argument("--timeout", type=float, default=8.0, help="HTTP timeout seconds")
    args = parser.parse_args()

    client = F10Client(timeout=args.timeout)
    checks = [
        ("company_profile", client.company_profile(args.code)),
        ("hot_topics", client.hot_topics(args.code)),
        ("announcements", client.announcements(args.code)),
        ("theme_market", client.theme_market(args.code, page_size=-1)),
        ("valuation", client.valuation(args.code)),
    ]

    for name, response in checks:
        if not response.ok:
            raise AssertionError(f"{name} returned ErrorCode={response.error_code}")
        row_counts = [table.count for table in response.tables]
        print(f"[OK] {name} tables={len(response.tables)} rows={row_counts}", flush=True)

    print("[OK] f10 smoke passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
