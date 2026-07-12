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


def _taxonomy_assignments(catalog: dict) -> dict[str, tuple[str, str | None]]:
    items = catalog["items"]
    item_ids = {item["id"] for item in items}
    assignments: dict[str, tuple[str, str | None]] = {}

    def assign(item_id: str, layer_id: str, group_id: str | None) -> None:
        assert item_id in item_ids
        assert item_id not in assignments
        assignments[item_id] = (layer_id, group_id)

    for layer in catalog["taxonomy"]["layers"]:
        for item_id in layer.get("item_ids", []):
            assign(item_id, layer["id"], None)
        for group in layer.get("groups", []):
            for item_id in group.get("item_ids", []):
                assign(item_id, layer["id"], group["id"])

    for layer in catalog["taxonomy"]["layers"]:
        for group in layer.get("groups", []):
            if source := group.get("source"):
                for item in items:
                    if item["id"] not in assignments and item["source"] == source:
                        assign(item["id"], layer["id"], group["id"])

    for layer in catalog["taxonomy"]["layers"]:
        if source := layer.get("source"):
            for item in items:
                if item["id"] not in assignments and item["source"] == source:
                    assign(item["id"], layer["id"], None)

    assert set(assignments) == item_ids
    return assignments


def test_pages_catalog_has_expected_public_interfaces() -> None:
    catalog = _catalog()
    items = catalog["items"]

    assert catalog["schema_version"] == 4
    assert len(items) == 64
    assert Counter(item["source"] for item in items) == {
        "7709": 28,
        "F10": 21,
        "Helper": 6,
        "MCP": 9,
    }
    assert len({item["id"] for item in items}) == len(items)


def test_pages_catalog_is_organized_by_source_and_call_level() -> None:
    catalog = _catalog()
    ordered_layers = catalog["taxonomy"]["layers"]
    layers = {layer["id"]: layer for layer in ordered_layers}
    assignments = _taxonomy_assignments(catalog)

    assert [(layer["id"], layer["label"]) for layer in ordered_layers] == [
        ("7709", "7709 行情接口"),
        ("7615", "7615 / F10 接口"),
        ("helpers", "Helpers 功能接口"),
        ("mcp", "MCP 工具"),
    ]
    assert Counter(layer_id for layer_id, _ in assignments.values()) == {
        "7709": 28,
        "7615": 21,
        "helpers": 6,
        "mcp": 9,
    }
    assert Counter(group_id for layer_id, group_id in assignments.values() if layer_id == "7709") == {
        "commands": 21,
        "convenience": 7,
    }
    assert Counter(group_id for layer_id, group_id in assignments.values() if layer_id == "7615") == {
        "entry": 1,
        "features": 20,
    }
    assert layers["helpers"]["source"] == "Helper" and "groups" not in layers["helpers"]
    assert layers["mcp"]["source"] == "MCP" and "groups" not in layers["mcp"]
    assert assignments["f10-generic-entry"] == ("7615", "entry")


def test_pages_catalog_covers_every_registered_7709_command() -> None:
    catalog = _catalog()
    tdx_layer = next(layer for layer in catalog["taxonomy"]["layers"] if layer["id"] == "7709")
    commands_group = next(group for group in tdx_layer["groups"] if group["id"] == "commands")
    binary_ids = set(commands_group["item_ids"])
    binary_protocols = [item["protocol"].lower() for item in catalog["items"] if item["id"] in binary_ids]

    assert len(COMMANDS) == len(binary_ids) == 21
    assert Counter(binary_protocols) == Counter(command.hex for command in COMMANDS.values())


def test_pages_catalog_links_to_existing_docs() -> None:
    for item in _catalog()["items"]:
        doc_path = REPO_ROOT / "docs" / item["doc"]
        assert doc_path.is_file(), item["id"]
        if anchor := item.get("doc_anchor"):
            assert f"{{#{anchor}}}" in doc_path.read_text(encoding="utf-8"), item["id"]


def test_quote_command_docs_explain_the_three_distinct_roles() -> None:
    command_map = (REPO_ROOT / "docs" / "COMMANDS_7709.md").read_text(encoding="utf-8")
    assert "## 三个行情命令的边界" in command_map
    assert "不能互换解析器" in command_map
    assert "client.get_quote(codes)" in command_map

    expected_roles = {
        "7709-批量快照.md": "无游标的一次性基础快照",
        "7709-增量刷新推送队列.md": "按代码和游标刷新行情",
        "7709-旧版批量行情.md": "无游标的旧版完整快照",
    }
    for filename, role in expected_roles.items():
        text = (REPO_ROOT / "docs" / "methods" / filename).read_text(encoding="utf-8")
        lower_text = text.lower()
        assert "## 与 `0x" in text
        assert role in text
        assert all(command in lower_text for command in ("0x054c", "0x0547", "0x053e"))

    items = {item["id"]: item for item in _catalog()["items"]}
    assert "一次性基础快照" in items["7709-quote-snapshots"]["summary"]
    assert "旧版完整快照" in items["7709-legacy-quotes"]["summary"]
    assert "代码和游标" in items["7709-quote-refresh"]["summary"]


def test_file_resource_catalog_documents_download_and_stats_parsing() -> None:
    items = {item["id"]: item for item in _catalog()["items"]}
    resource = items["7709-file-content"]
    method_doc = (REPO_ROOT / "docs" / resource["doc"]).read_text(encoding="utf-8")

    assert resource["protocol"].lower() == "0x06b9"
    assert all(name in resource["api"] for name in ("read()", "download_file()", "read_stats()"))
    assert "TdxStatsResource" in resource["return_model"]
    assert "不是新的二进制命令" in method_doc
    assert all(name in method_doc for name in ("tdxstat.cfg", "tdxstat2.cfg", "free_float_shares_10k", "open_amount_10k"))


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
    assert '"wrapper/helpers": "helpers"' in app
    assert "catalog-tree-leaf" in app


def test_readme_promotes_the_static_pages_catalog() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert '<a href="https://electkismet.github.io/eltdx/"><strong>接口一览</strong></a>' in readme
    assert "<strong>在线文档</strong>" not in readme
    assert 'src=".github/assets/eltdx-readme-banner.png"' in readme
    assert README_BANNER_PATH.is_file()
