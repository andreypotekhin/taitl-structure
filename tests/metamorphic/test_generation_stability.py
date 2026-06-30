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
        "structure_generated/orders/pyspark/schemas/common.py",
        "structure_generated/orders/pyspark/schemas/customer.py",
        "structure_generated/orders/pyspark/schemas/order.py",
        "structure_generated/orders/pyspark/schemas/product.py",
        "structure_generated/orders/pyspark/schemas/promotion.py",
        "structure_generated/orders/pyspark/schemas/shipment.py",
        "structure_generated/orders/pyspark/transforms/order.py",
        "structure_generated/orders/traceability/transforms/order.EnrichOrders.json",
        "structure_generated/orders/traceability/__init__.py",
        "structure_generated/orders/traceability/transforms/__init__.py",
    ]


def test_orders_example_generation_keeps_public_behavior_fragments_stable() -> None:
    transform = render_orders_example()["structure_generated/orders/pyspark/transforms/order.py"]

    assert "class EnrichOrdersGenerated:" in transform
    assert "from orders.transforms.order import EnrichOrders" in transform
    assert (
        "orders = self._impl.use_current_orders(orders=orders, inputs=inputs, spark=self.spark, ctx=self.ctx)"
        in transform
    )
    assert 'customers_joined = F.broadcast(customers.alias("customers"))' in transform
    assert 'promotions_joined = promotions.alias("promotions")' in transform
    assert 'published = self._impl.add_quality_columns(published=published, spark=self.spark, ctx=self.ctx)' in transform
