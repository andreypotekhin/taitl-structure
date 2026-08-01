from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark import PySpark


def _recipe(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False).lowered


def test_v1_step_renderer_renders_before_hook_against_current_input() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    recipe = _recipe(EnrichOrders)
    text = PySpark.render.step()(recipe.steps[0], current="orders")

    assert "        orders = self._impl.use_current_orders(orders=orders, spark=self.spark, ctx=self.ctx)" in text
    assert '        orders = orders.alias("order_raw")' in text
    assert "orders=orders" in text


def test_v1_step_renderer_renders_join_projection_and_validation() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    recipe = _recipe(EnrichOrders)
    text = PySpark.render.step()(recipe.steps[1], current="orders")

    assert '        # Step method: add_customer' in text
    assert '        orders = orders.alias("order_normalized")' in text
    assert '        customers_joined = F.broadcast(customers.alias("customers"))' in text
    assert '            "left",' in text
    assert 'F.lower(F.trim(F.col("customers.id"))) == F.col("order_normalized.customer_id")' in text
    assert '            F.col("customers.name").alias("customer_name"),' in text
    assert '        assert_schema(orders, ORDER_WITH_CUSTOMER_SCHEMA, name="OrderWithCustomer", mode="strict")' in text


def test_v1_step_renderer_renders_hooks_and_project_output_validation() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    recipe = _recipe(EnrichOrders)
    text = PySpark.render.step()(recipe.steps[5], current="orders")

    assert '        # Step method: publish' in text
    assert (
        '        published = self._impl.add_quality_columns(published=published, spark=self.spark, ctx=self.ctx)'
        in text
    )
    assert (
        '        assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="allow_extra_columns")'
        in text
    )
    assert "        published = project_schema(published, ORDER_PUBLISHED_SCHEMA)" in text
    assert text.count('assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")') == 1
