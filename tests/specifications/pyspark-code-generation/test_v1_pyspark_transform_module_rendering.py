import sys

from structure import Schema, String, Transform, field, input, output, step, transform
from structure.app.cli.commands.RenderExplainReport import render_explain_report
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class CacheRaw(Schema):
    id = field(String(), nullable=False)
    status = field(String(), nullable=True)


class CachePublished(Schema):
    id = field(String(), nullable=False)
    status = field(String(), nullable=True)


@transform
class CachePublishedOrders(Transform):
    orders = input(CacheRaw)
    published = output(CachePublished)

    @step(cache=True)
    def publish(self, order: CacheRaw) -> CachePublished:
        return CachePublished(id=order.id, status=order.status)


def test_v1_transform_module_renderer_is_spark_free() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    before = {name for name in sys.modules if name.startswith("pyspark")}

    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
    )

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert text.startswith("from pyspark.sql import DataFrame, SparkSession\n")


def test_v1_transform_module_renderer_renders_class_runtime_shape() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
    )

    assert "from testing.model.v1.orders.transforms.order import EnrichOrders" in text
    assert (
        "from testing.model.v1.structure_generated.runtime.schema_assert import "
        "TransformResult, assert_schema, project_schema" in text
    )
    assert "class EnrichOrdersGenerated:" in text
    assert "        self._impl = EnrichOrders()" in text
    assert "        orders: DataFrame," in text
    assert '        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")' in text
    assert "HookInputs" not in text


def test_v1_transform_module_renderer_composes_steps_and_final_return() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
    )

    assert "        # Step method: normalize" in text
    assert "        # Step method: add_customer" in text
    assert "        # Step method: add_product" in text
    assert "        # Step method: add_promotion" in text
    assert "        # Step method: publish" in text
    assert "        orders = self._impl.use_current_orders(orders=_input_orders, spark=self.spark, ctx=self.ctx)" in text
    assert (
        "        orders = self._impl.note_lookup_inputs(orders=orders, customers=_input_customers, "
        "products=_input_products, spark=self.spark, ctx=self.ctx)" in text
    )
    assert "        published = project_schema(published, ORDER_PUBLISHED_SCHEMA)" in text
    assert text.count('assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")') == 2
    assert text.rstrip().endswith(
        '        return TransformResult({"published": published}, single=True, '
        'schema={"published": ORDER_PUBLISHED_SCHEMA})'
    )


def test_v2_cache_directive_renders_as_post_projection_persist() -> None:
    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(CachePublishedOrders)),
        source_transform="tests.CachePublishedOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules={
            CacheRaw: "testing.model.v1.structure_generated.cache.pyspark.schemas.order",
            CachePublished: "testing.model.v1.structure_generated.cache.pyspark.schemas.order",
        },
    )

    assert "        orders = orders.select(" in text
    assert "        orders = orders.persist()" in text
    assert text.index("        orders = orders.select(") < text.index("        orders = orders.persist()")
    assert text.index("        orders = orders.persist()") < text.index("        # Step method: published")


def test_v2_cache_directive_is_visible_in_explain_output() -> None:
    text = render_explain_report(CachePublishedOrders)

    assert "operations: cache(row_preserving)" in text


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
