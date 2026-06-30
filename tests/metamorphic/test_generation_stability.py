from __future__ import annotations

from helpers.example_projects import render_orders_example


def test_orders_example_generation_is_byte_identical_across_repeated_runs() -> None:
    assert render_orders_example() == render_orders_example()


def test_orders_example_generated_file_order_is_deterministic() -> None:
    paths = list(render_orders_example())

    assert paths == [
        "structure_generated/orders/__init__.py",
        "structure_generated/orders/pyspark/__init__.py",
        "structure_generated/orders/pyspark/schemas/__init__.py",
        "structure_generated/orders/pyspark/transforms/__init__.py",
        "structure_generated/orders/runtime/__init__.py",
        "structure_generated/orders/runtime/schema_assert.py",
        "structure_generated/orders/pyspark/schemas/schemas.py",
        "structure_generated/orders/pyspark/transforms/transforms.py",
        "structure_generated/orders/traceability/transforms/transforms.PublishOrders.json",
    ]


def test_orders_example_generation_keeps_public_behavior_fragments_stable() -> None:
    transform = render_orders_example()["structure_generated/orders/pyspark/transforms/transforms.py"]

    assert 'orders = orders.alias("order_raw")' in transform
    assert 'orders = orders.where((F.col("order_raw.id").isNotNull())' in transform
    assert 'F.coalesce(F.col("order_raw.total").cast("decimal(12,2)"), F.lit(0)).alias("total")' in transform
    assert 'F.lit(\'ready\').alias("status")' in transform
