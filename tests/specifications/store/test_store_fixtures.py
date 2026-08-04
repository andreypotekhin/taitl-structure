from __future__ import annotations

import csv
from pathlib import Path

FIXTURES = Path(__file__).resolve().parents[3] / "examples" / "store" / "fixtures"

CORE_FILES = {
    "orders.csv",
    "customers.csv",
    "products.csv",
    "blocked_products.csv",
    "promotions.csv",
    "shipments.csv",
    "warehouses.csv",
    "inventory_positions.csv",
    "inbound_inventory.csv",
    "lead_times.csv",
    "substitution_rules.csv",
    "service_targets.csv",
    "taxonomy_nodes.csv",
    "product_taxonomy.csv",
    "recommendation_requests.csv",
    "merchandising_policies.csv",
    "merchandising_boosts.csv",
    "merchandising_suppressions.csv",
    "session_events.csv",
    "recommendation_impressions.csv",
    "recommendation_clicks.csv",
    "user_feature_preferences.csv",
    "recommendation_experiments.csv",
    "recommendation_evaluation_batches.csv",
}


def _rows(name: str) -> list[dict[str, str]]:
    with (FIXTURES / name).open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def test_store_fixtures_cover_core_source_relations() -> None:
    assert CORE_FILES <= {path.name for path in FIXTURES.glob("*.csv")}

    for name in CORE_FILES:
        assert _rows(name), name


def test_store_fixture_rows_cover_representative_model_branches() -> None:
    orders = _rows("orders.csv")
    products = _rows("products.csv")
    inventory = _rows("inventory_positions.csv")
    requests = _rows("recommendation_requests.csv")

    order_lines = {(row["id"], row["line_number"]) for row in orders}
    assert {("o-100", "1"), ("o-100", "2")} <= order_lines
    assert any(row["promotion_code"] == "SUMMER" for row in orders)
    assert any(row["active"] == "false" for row in products)
    assert {row["on_hand_quantity"] for row in inventory} >= {"0", "8"}
    assert {row["id"] for row in requests} >= {"r-100", "r-101"}
