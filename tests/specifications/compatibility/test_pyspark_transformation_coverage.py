import json
from pathlib import Path
from typing import Any

import structure
from structure.plugin import pyspark

ROOT = Path(__file__).resolve().parents[3]
RESOURCES = ROOT / "src/structure/plugin/pyspark/resources"
INVENTORY = RESOURCES / "pyspark-transformation-inventory.json"
CATALOG = RESOURCES / "pyspark-transformation-coverage.json"
VALID_STATUSES = {"supported", "scheduled", "deferred", "design-gated", "unsupported"}


def test_pyspark_transformation_catalog_classifies_the_entire_local_inventory() -> None:
    inventory = _load(INVENTORY)
    catalog = _load(CATALOG)
    inventory_ids = [entry["id"] for entry in inventory["apis"]]
    catalog_ids = [entry["id"] for entry in catalog["entries"]]

    assert len(inventory_ids) == len(set(inventory_ids))
    assert len(catalog_ids) == len(set(catalog_ids))
    assert set(catalog_ids) == set(inventory_ids)
    assert inventory["excluded_categories"]


def test_pyspark_transformation_catalog_entries_are_actionable() -> None:
    for entry in _load(CATALOG)["entries"]:
        assert entry["status"] in VALID_STATUSES
        assert entry["structure"]
        assert entry["profile"]
        assert entry["contract"]
        assert entry["notes"]
        assert entry["evidence"]
        assert "4.1" not in entry["profile"]
        for evidence in entry["evidence"]:
            assert (ROOT / evidence).is_file(), f"{entry['id']} evidence is missing: {evidence}"


def test_supported_catalog_entries_name_exported_structure_api_evidence() -> None:
    for entry in _load(CATALOG)["entries"]:
        if entry["status"] != "supported":
            continue
        assert entry["public_symbols"], f"{entry['id']} lacks a public Structure spelling"
        assert all(hasattr(structure, symbol) or hasattr(pyspark, symbol) for symbol in entry["public_symbols"])
        assert any(
            "tests/" in evidence for evidence in entry["evidence"]
        ), f"{entry['id']} lacks parity or generated-code evidence"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
