from __future__ import annotations

from decimal import Decimal

from helpers.example_projects import render_orders_example


def test_orders_example_matches_independent_reference_rows() -> None:
    raw = [
        {"id": "o-1", "customer_id": "c-1", "total": "1250.50"},
        {"id": "o-2", "customer_id": "c-2", "total": ""},
        {"id": "", "customer_id": "c-3", "total": "8.00"},
    ]

    assert _reference_publish(raw) == [
        {"id": "o-1", "customer_id": "c-1", "total": Decimal("1250.50"), "status": "ready"},
        {"id": "o-2", "customer_id": "c-2", "total": Decimal("0"), "status": "ready"},
    ]


def test_orders_generated_code_matches_independent_reference_operations() -> None:
    transform = render_orders_example()["structure_generated/orders/pyspark/transforms/transforms.py"]

    reference_fragments = [
        'assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")',
        'orders = orders.where((F.col("order_raw.id").isNotNull()) & (F.col("order_raw.customer_id").isNotNull()))',
        'F.coalesce(F.col("order_raw.total").cast("decimal(12,2)"), F.lit(0)).alias("total")',
        'F.lit(\'ready\').alias("status")',
        'assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")',
    ]

    for fragment in reference_fragments:
        assert fragment in transform


def _reference_publish(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    return [
        {
            "id": row["id"],
            "customer_id": row["customer_id"],
            "total": Decimal(row["total"] or "0"),
            "status": "ready",
        }
        for row in rows
        if row["id"] and row["customer_id"]
    ]
