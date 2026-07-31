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
        "examples/structure_generated/store/pyspark/schemas/summary.py",
        "examples/structure_generated/store/pyspark/schemas/demand.py",
        "examples/structure_generated/store/pyspark/schemas/inventory.py",
        "examples/structure_generated/store/pyspark/schemas/plan.py",
        "examples/structure_generated/store/pyspark/schemas/workflow.py",
        "examples/structure_generated/store/pyspark/schemas/reconciliation.py",
        "examples/structure_generated/store/pyspark/schemas/catalog.py",
        "examples/structure_generated/store/pyspark/schemas/evaluation.py",
        "examples/structure_generated/store/pyspark/schemas/feedback.py",
        "examples/structure_generated/store/pyspark/schemas/intermediate.py",
        "examples/structure_generated/store/pyspark/schemas/policy.py",
        "examples/structure_generated/store/pyspark/schemas/recommendation.py",
        "examples/structure_generated/store/pyspark/schemas/order.py",
        "examples/structure_generated/store/pyspark/schemas/product.py",
        "examples/structure_generated/store/pyspark/schemas/promotion.py",
        "examples/structure_generated/store/pyspark/schemas/shipment.py",
        "examples/structure_generated/store/pyspark/transforms/order.py",
        "examples/structure_generated/store/traceability/transforms/order.EnrichOrders.json",
        "examples/structure_generated/store/pyspark/transforms/demand.py",
        "examples/structure_generated/store/traceability/transforms/demand.PrepareOrderDemand.json",
        "examples/structure_generated/store/pyspark/transforms/plan.py",
        "examples/structure_generated/store/traceability/transforms/plan.PlanFulfillment.json",
        "examples/structure_generated/store/pyspark/transforms/reconcile.py",
        "examples/structure_generated/store/traceability/transforms/reconcile.ReconcileFulfillmentPlan.json",
        "examples/structure_generated/store/pyspark/transforms/summarize.py",
        "examples/structure_generated/store/traceability/transforms/summarize.FulfillmentAnalytics.json",
        "examples/structure_generated/store/pyspark/transforms/fulfillment_workflow.py",
        "examples/structure_generated/store/traceability/transforms/fulfillment_workflow.Fulfillment.json",
        "examples/structure_generated/store/pyspark/transforms/prepare_catalog.py",
        "examples/structure_generated/store/traceability/transforms/prepare_catalog.PrepareCatalog.json",
        "examples/structure_generated/store/pyspark/transforms/admit.py",
        "examples/structure_generated/store/traceability/transforms/admit.SelectRecommendationCandidates.json",
        "examples/structure_generated/store/pyspark/transforms/recommender_workflow.py",
        "examples/structure_generated/store/traceability/transforms/recommender_workflow.Recommender.json",
        "examples/structure_generated/store/pyspark/transforms/merchandising_workflow.py",
        "examples/structure_generated/store/traceability/transforms/merchandising_workflow.Merchandising.json",
        "examples/structure_generated/store/pyspark/transforms/build_signals.py",
        "examples/structure_generated/store/traceability/transforms/build_signals.BuildRecommendationSignals.json",
        "examples/structure_generated/store/pyspark/transforms/clicks_workflow.py",
        "examples/structure_generated/store/traceability/transforms/clicks_workflow.EvaluateMerchandising.json",
        "examples/structure_generated/store/pyspark/transforms/rowset_join_examples.py",
        "examples/structure_generated/store/traceability/transforms/rowset_join_examples.RowsetJoinExamples.json",
        "examples/structure_generated/store/pyspark/transforms/order_analytics_workflow.py",
        "examples/structure_generated/store/traceability/transforms/order_analytics_workflow.OrderAnalytics.json",
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
    assert "F.filter(" in transform
    assert 'F.transform(F.col("order_raw.tags"), lambda item: F.lower(F.trim(item)))' in transform
    assert "F.map_filter(" in transform
    assert 'F.transform_values(F.col("order_raw.attributes"), lambda key, value: F.lower(F.trim(value)))' in transform

    rowset = render_store_example()["examples/structure_generated/store/pyspark/transforms/rowset_join_examples.py"]

    assert "class RowsetJoinExamplesGenerated(" in rowset
    assert '"full"' in rowset
    assert '"right"' in rowset
    assert ".crossJoin(" in rowset

    analytics = render_store_example()[
        "examples/structure_generated/store/pyspark/transforms/order_analytics_workflow.py"
    ]

    assert "class OrderAnalyticsGenerated(" in analytics
    assert "product__product_summary = (" in analytics
    assert "product_summary.groupBy(" in analytics
    assert 'F.avg(F.col("order_fulfillment.quantity")).cast(T.DoubleType()).alias("avg_units")' in analytics

    advanced = render_store_example()["examples/structure_generated/store/pyspark/transforms/adv_analytics.py"]

    assert "class AdvancedOrderAnalyticsGenerated:" in advanced
    assert "revenue_rollups.rollup(" in advanced
    assert "product_cubes.cube(" in advanced
    assert "Window.partitionBy" in advanced
    assert "F.exists(" in advanced
    assert "F.map_from_entries(" in advanced
    assert "F.map_entries(" in advanced
