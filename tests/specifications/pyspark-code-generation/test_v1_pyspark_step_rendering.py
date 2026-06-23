from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import pyspark


def test_v1_step_renderer_renders_before_hook_against_current_input() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = pyspark.plan.lower()(compile_transform(EnrichOrders))
    text = pyspark.render.step()(recipe.steps[0], current="orders")

    assert (
        "        df = self._impl.use_current_orders(df=orders, inputs=inputs, spark=self.spark, ctx=self.ctx)" in text
    )
    assert '        df = df.alias("order_raw")' in text
    assert "df=orders" in text
    assert "df=df, inputs=inputs" not in text


def test_v1_step_renderer_renders_join_projection_and_validation() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = pyspark.plan.lower()(compile_transform(EnrichOrders))
    text = pyspark.render.step()(recipe.steps[1], current="df")

    assert '        # Subtransform: add_customer' in text
    assert '        df = df.alias("order_normalized")' in text
    assert '        customers_df = F.broadcast(customers.alias("customers"))' in text
    assert '            "left",' in text
    assert 'F.lower(F.trim(F.col("customers.id"))) == F.col("order_normalized.customer_id")' in text
    assert '            F.col("customers.name").alias("customer_name"),' in text
    assert '        assert_schema(df, ORDER_WITH_CUSTOMER_SCHEMA, name="OrderWithCustomer", mode="strict")' in text


def test_v1_step_renderer_renders_hooks_and_project_output_validation() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = pyspark.plan.lower()(compile_transform(EnrichOrders))
    text = pyspark.render.step()(recipe.steps[4], current="df")

    assert '        # Subtransform: publish' in text
    assert '        df = self._impl.add_quality_columns(df=df, spark=self.spark, ctx=self.ctx)' in text
    assert (
        '        assert_schema(df, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="allow_extra_columns")' in text
    )
    assert "        df = project_schema(df, ORDER_PUBLISHED_SCHEMA)" in text
    assert text.count('assert_schema(df, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")') == 1
