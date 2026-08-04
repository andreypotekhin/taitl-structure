from __future__ import annotations

from helpers.example_projects import render_store_example


def test_store_example_generation_is_byte_identical_across_repeated_runs() -> None:
    assert render_store_example() == render_store_example()


def test_store_example_generated_file_order_is_deterministic() -> None:
    paths = list(render_store_example())
    non_docs = [path for path in paths if "/docs/" not in path and "/traceability/" not in path]

    assert non_docs[:5] == [
        "examples/structure_generated/store/__init__.py",
        "examples/structure_generated/store/pyspark/__init__.py",
        "examples/structure_generated/store/pyspark/schemas/__init__.py",
        "examples/structure_generated/store/pyspark/transforms/__init__.py",
        "examples/structure_generated/store/runtime/__init__.py",
    ]
    assert non_docs.index("examples/structure_generated/store/runtime/schema_assert.py") < non_docs.index(
        "examples/structure_generated/store/pyspark/schemas/adv_analytics.py"
    )
    assert all(
        path.startswith("examples/structure_generated/store/pyspark/transforms/examples/store/transforms/")
        for path in non_docs
        if "/pyspark/transforms/" in path and not path.endswith("/__init__.py")
    )
    assert (
        "examples/structure_generated/store/pyspark/transforms/examples/store/transforms/fulfillment/demand/prepare.py"
        in non_docs
    )
    assert (
        "examples/structure_generated/store/pyspark/transforms/examples/store/transforms/catalog/prepare_catalog.py"
        in non_docs
    )
    assert not any("/docs/" in path or "/traceability/" in path for path in paths)


def test_store_example_generation_keeps_public_behavior_fragments_stable() -> None:
    transform = render_store_example()[
        "examples/structure_generated/store/pyspark/transforms/examples/store/transforms/orders/enrich.py"
    ]

    assert "class EnrichOrdersGenerated:" in transform
    assert "from examples.store.transforms.orders.enrich import EnrichOrders" not in transform
    assert "self._impl." not in transform
    assert 'customers_joined = F.broadcast(customers.alias("customers"))' in transform
    assert 'promotions_joined = promotions.alias("promotions")' in transform
    assert '# Step method: discard_negative_totals' in transform
    assert 'assert_schema(orders, ORDER_FULFILLMENT_SCHEMA, name="OrderFulfillment", mode="strict")' in transform
    assert "F.filter(" in transform
    assert 'F.transform(F.col("order_raw.tags"), lambda item: F.lower(F.trim(item)))' in transform
    assert "F.map_filter(" in transform
    assert 'F.transform_values(F.col("order_raw.attributes"), lambda key, value: F.lower(F.trim(value)))' in transform

    rowset = render_store_example()[
        "examples/structure_generated/store/pyspark/transforms/examples/store/transforms/rowset_joins/"
        "rowset_join_examples.py"
    ]

    assert "class RowsetJoinExamplesGenerated(" in rowset
    assert '"full"' in rowset
    assert '"right"' in rowset
    assert ".crossJoin(" in rowset

    analytics = render_store_example()[
        "examples/structure_generated/store/pyspark/transforms/examples/store/transforms/analytics/orders/workflow.py"
    ]

    assert "class OrderAnalyticsGenerated(" in analytics
    assert "product__product_summary = (" in analytics
    assert "product_summary.groupBy(" in analytics
    assert 'F.avg(F.col("order_fulfillment.quantity")).cast(T.DoubleType()).alias("avg_units")' in analytics

    advanced = render_store_example()[
        "examples/structure_generated/store/pyspark/transforms/examples/store/transforms/adv_analytics.py"
    ]

    assert "class AdvancedOrderAnalyticsGenerated:" in advanced
    assert "revenue_rollups.rollup(" in advanced
    assert "product_cubes.cube(" in advanced
    assert "Window.partitionBy" in advanced
    assert "F.exists(" in advanced
    assert "F.map_from_entries(" in advanced
    assert "F.map_entries(" in advanced
