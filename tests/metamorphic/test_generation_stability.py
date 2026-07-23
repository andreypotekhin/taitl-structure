from __future__ import annotations

from helpers.example_projects import render_store_example


def test_store_example_generation_is_byte_identical_across_repeated_runs() -> None:
    assert render_store_example() == render_store_example()


def test_store_example_generated_file_order_is_deterministic() -> None:
    paths = list(render_store_example())
    non_docs = [path for path in paths if "/docs/" not in path]
    docs = [path for path in paths if "/docs/" in path]
    markdown_docs = sorted(path for path in docs if path.endswith(".md"))
    json_docs = sorted(path for path in docs if path.endswith(".json"))

    assert non_docs == [
        "examples/structure_generated/store/__init__.py",
        "examples/structure_generated/store/pyspark/__init__.py",
        "examples/structure_generated/store/pyspark/schemas/__init__.py",
        "examples/structure_generated/store/pyspark/transforms/__init__.py",
        "examples/structure_generated/store/runtime/__init__.py",
        "examples/structure_generated/store/runtime/schema_assert.py",
        "examples/structure_generated/store/pyspark/schemas/adv_analytics.py",
        "examples/structure_generated/store/pyspark/schemas/analytics.py",
        "examples/structure_generated/store/pyspark/schemas/common.py",
        "examples/structure_generated/store/pyspark/schemas/customer.py",
        "examples/structure_generated/store/pyspark/schemas/order.py",
        "examples/structure_generated/store/pyspark/schemas/product.py",
        "examples/structure_generated/store/pyspark/schemas/promotion.py",
        "examples/structure_generated/store/pyspark/schemas/shipment.py",
        "examples/structure_generated/store/pyspark/transforms/order.py",
        "examples/structure_generated/store/traceability/transforms/order.EnrichOrders.json",
        "examples/structure_generated/store/pyspark/transforms/rowset_join.py",
        "examples/structure_generated/store/traceability/transforms/rowset_join.RowsetJoinExamples.json",
        "examples/structure_generated/store/pyspark/transforms/analytics.py",
        "examples/structure_generated/store/traceability/transforms/analytics.OrderAnalytics.json",
        "examples/structure_generated/store/pyspark/transforms/adv_analytics.py",
        "examples/structure_generated/store/traceability/transforms/adv_analytics.AdvancedOrderAnalytics.json",
        "examples/structure_generated/store/traceability/__init__.py",
        "examples/structure_generated/store/traceability/transforms/__init__.py",
    ]
    assert docs == markdown_docs + json_docs


def test_store_example_generation_keeps_public_behavior_fragments_stable() -> None:
    transform = render_store_example()["examples/structure_generated/store/pyspark/transforms/order.py"]

    assert "class EnrichOrdersGenerated:" in transform
    assert "from examples.store.transforms.order import EnrichOrders" not in transform
    assert "self._impl." not in transform
    assert 'customers_joined = F.broadcast(customers.alias("customers"))' in transform
    assert 'promotions_joined = promotions.alias("promotions")' in transform
    assert '# Step method: discard_negative_totals' in transform
    assert 'assert_schema(orders, ORDER_FULFILLMENT_SCHEMA, name="OrderFulfillment", mode="strict")' in transform
    assert 'F.filter(F.transform(F.col("order_raw.tags"), lambda item: F.lower(F.trim(item)))' in transform
    assert (
        'F.map_filter(F.transform_values(F.col("order_raw.attributes"), '
        'lambda key, value: F.lower(F.trim(value)))' in transform
    )

    rowset = render_store_example()["examples/structure_generated/store/pyspark/transforms/rowset_join.py"]

    assert "class RowsetJoinExamplesGenerated:" in rowset
    assert '"full"' in rowset
    assert '"right"' in rowset
    assert ".crossJoin(" in rowset

    analytics = render_store_example()["examples/structure_generated/store/pyspark/transforms/analytics.py"]

    assert "class OrderAnalyticsGenerated:" in analytics
    assert 'product_summary = product_summary.groupBy(' in analytics
    assert 'F.avg(F.col("order_fulfillment.quantity")).cast(T.DoubleType()).alias("avg_units")' in analytics

    advanced = render_store_example()["examples/structure_generated/store/pyspark/transforms/adv_analytics.py"]

    assert "class AdvancedOrderAnalyticsGenerated:" in advanced
    assert "revenue_rollups = revenue_rollups.rollup(" in advanced
    assert "product_cubes = product_cubes.cube(" in advanced
    assert "Window.partitionBy" in advanced
    assert "F.exists(" in advanced
    assert "F.map_from_entries(F.map_entries(" in advanced
