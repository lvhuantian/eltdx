"""Command-line entry points for eltdx."""

from __future__ import annotations

from eltdx.f10_smoke import main as f10_smoke_main
from eltdx.smoke import main as smoke_main


def smoke() -> int:
    return smoke_main()


def f10_smoke() -> int:
    return f10_smoke_main()
