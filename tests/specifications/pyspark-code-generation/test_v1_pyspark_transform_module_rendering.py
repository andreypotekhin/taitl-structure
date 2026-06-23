import sys

from structure.app.target.pyspark.api import lower_pyspark_plan, render_pyspark_transform_module
from structure.app.dsl.api import compile_transform


def test_v1_transform_module_renderer_is_spark_free() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    before = {name for name in sys.modules if name.startswith("pyspark")}

    text = render_pyspark_transform_module(
        lower_pyspark_plan(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
    )

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert text.startswith("from pyspark.sql import DataFrame, SparkSession\n")


def test_v1_transform_module_renderer_renders_class_runtime_shape() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    text = render_pyspark_transform_module(
        lower_pyspark_plan(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
    )

    assert "from testing.model.v1.orders.transforms.order import EnrichOrders" in text
    assert (
        "from testing.model.v1.structure_generated.runtime.schema_assert import "
        "TransformResult, assert_schema, project_schema, HookInputs" in text
    )
    assert "class EnrichOrdersGenerated:" in text
    assert "        self._impl = EnrichOrders()" in text
    assert "        orders: DataFrame," in text
    assert '        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")' in text
    assert "        inputs = HookInputs(" in text
    assert "            promotions=promotions," in text


def test_v1_transform_module_renderer_composes_steps_and_final_return() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    text = render_pyspark_transform_module(
        lower_pyspark_plan(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
    )

    assert "        # Subtransform: normalize" in text
    assert "        # Subtransform: add_customer" in text
    assert "        # Subtransform: add_product" in text
    assert "        # Subtransform: add_promotion" in text
    assert "        # Subtransform: publish" in text
    assert (
        "        df = self._impl.use_current_orders(df=orders, inputs=inputs, spark=self.spark, ctx=self.ctx)" in text
    )
    assert "        df = project_schema(df, ORDER_PUBLISHED_SCHEMA)" in text
    assert text.count('assert_schema(df, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")') == 2
    assert text.rstrip().endswith('        return TransformResult({"df": df_df}, single=True)')


def _schema_modules() -> dict[type, str]:
    from testing.model.v1.orders.schemas.customer import Customer
    from testing.model.v1.orders.schemas.order import (
        OrderNormalized,
        OrderPublished,
        OrderRaw,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
    )
    from testing.model.v1.orders.schemas.product import Product
    from testing.model.v1.orders.schemas.promotion import Promotion

    order_module = "testing.model.v1.structure_generated.orders.pyspark.schemas.order"
    return {
        OrderRaw: order_module,
        OrderNormalized: order_module,
        OrderWithCustomer: order_module,
        OrderWithProduct: order_module,
        OrderWithPromotion: order_module,
        OrderPublished: order_module,
        Customer: "testing.model.v1.structure_generated.orders.pyspark.schemas.customer",
        Product: "testing.model.v1.structure_generated.orders.pyspark.schemas.product",
        Promotion: "testing.model.v1.structure_generated.orders.pyspark.schemas.promotion",
    }
