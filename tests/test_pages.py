from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from eltdx.protocol import COMMANDS


REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "docs" / "assets" / "interface-catalog-data.js"


def _catalog() -> dict:
    text = CATALOG_PATH.read_text(encoding="utf-8")
    prefix = "window.ELTDX_CATALOG = "
    assert text.startswith(prefix)
    payload = text[len(prefix) :].strip()
    assert payload.endswith(";")
    return json.loads(payload[:-1])


def test_pages_catalog_has_expected_public_interfaces() -> None:
    catalog = _catalog()
    items = catalog["items"]

    assert catalog["schema_version"] == 2
    assert len(items) == 64
    assert Counter(item["source"] for item in items) == {
        "7709": 28,
        "F10": 21,
        "Helper": 6,
        "MCP": 9,
    }
    assert len({item["id"] for item in items}) == len(items)


def test_pages_catalog_covers_every_registered_7709_command() -> None:
    protocol_text = " ".join(item["protocol"].lower() for item in _catalog()["items"] if item["source"] == "7709")

    for command in COMMANDS.values():
        assert command.hex in protocol_text


def test_pages_catalog_links_to_existing_docs() -> None:
    for item in _catalog()["items"]:
        doc_path = REPO_ROOT / "docs" / item["doc"]
        assert doc_path.is_file(), item["id"]
        if anchor := item.get("doc_anchor"):
            assert f"{{#{anchor}}}" in doc_path.read_text(encoding="utf-8"), item["id"]


def test_pages_remains_static_and_outside_runtime_dependencies() -> None:
    app = (REPO_ROOT / "docs" / "assets" / "interface-catalog.js").read_text(encoding="utf-8")
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    gitignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")

    assert "fetch(" not in app
    assert "XMLHttpRequest" not in app
    assert "WebSocket" not in app
    assert "dependencies = []" in pyproject
    assert "site/" in gitignore.splitlines()
