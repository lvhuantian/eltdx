from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from eltdx.protocol import COMMANDS


REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "docs" / "assets" / "interface-catalog-data.js"
README_BANNER_PATH = REPO_ROOT / ".github" / "assets" / "eltdx-readme-banner.png"


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

    assert catalog["schema_version"] == 3
    assert len(items) == 64
    assert Counter(item["source"] for item in items) == {
        "7709": 28,
        "F10": 21,
        "Helper": 6,
        "MCP": 9,
    }
    assert len({item["id"] for item in items}) == len(items)


def test_pages_catalog_has_two_exclusive_interface_layers() -> None:
    catalog = _catalog()
    items = catalog["items"]
    item_ids = {item["id"] for item in items}
    layers = {layer["id"]: layer for layer in catalog["taxonomy"]["layers"]}

    assert set(layers) == {"binary", "wrapper"}
    assert layers["binary"]["label"] == "二进制接口解析"
    assert layers["wrapper"]["label"] == "封装接口"

    binary_groups = layers["binary"]["groups"]
    binary_ids = [item_id for group in binary_groups for item_id in group["item_ids"]]
    wrapper_ids = item_ids - set(binary_ids)

    assert len(binary_ids) == len(set(binary_ids)) == 21
    assert set(binary_ids) <= item_ids
    assert len(wrapper_ids) == 43
    assert Counter(item["source"] for item in items if item["id"] in wrapper_ids) == {
        "7709": 7,
        "F10": 21,
        "Helper": 6,
        "MCP": 9,
    }
    assert {group["source"] for group in layers["wrapper"]["groups"]} == {"7709", "F10", "Helper", "MCP"}


def test_pages_catalog_covers_every_registered_7709_command() -> None:
    catalog = _catalog()
    binary_ids = {
        item_id
        for group in catalog["taxonomy"]["layers"][0]["groups"]
        for item_id in group["item_ids"]
    }
    binary_protocols = [item["protocol"].lower() for item in catalog["items"] if item["id"] in binary_ids]

    assert len(COMMANDS) == len(binary_ids) == 21
    assert Counter(binary_protocols) == Counter(command.hex for command in COMMANDS.values())


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


def test_pages_catalog_ui_exposes_taxonomy_navigation() -> None:
    page = (REPO_ROOT / "docs" / "index.md").read_text(encoding="utf-8")
    app = (REPO_ROOT / "docs" / "assets" / "interface-catalog.js").read_text(encoding="utf-8")

    assert "data-interface-tree" in page
    assert "data-interface-scope-select" in page
    assert "window.location.hash" in app
    assert 'layer.id + "/" + group.id' in app


def test_readme_promotes_the_static_pages_catalog() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert '<a href="https://electkismet.github.io/eltdx/"><strong>接口一览</strong></a>' in readme
    assert "<strong>在线文档</strong>" not in readme
    assert 'src=".github/assets/eltdx-readme-banner.png"' in readme
    assert README_BANNER_PATH.is_file()
