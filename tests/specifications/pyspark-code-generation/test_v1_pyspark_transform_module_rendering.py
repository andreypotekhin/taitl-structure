import ast
import sys
from typing import Any

from structure import *
from structure.app.cli.commands.RenderExplainReport import render_explain_report
from structure.app.target.pyspark.api import PySpark


class CacheRaw(Schema):
    id = field.string(nullable=False)
    status = field.string(nullable=True)


class CachePublished(Schema):
    id = field.string(nullable=False)
    status = field.string(nullable=True)


class UdfRaw(Schema):
    id = field.string(nullable=False)


@transform
class UdfPublished(Transform):
    rows = input(UdfRaw)
    published = output(UdfRaw)

    @special(type="udf", return_type=types.string())
    def normalize(value: Any):
        return value.strip().lower()

    def publish(self, row: UdfRaw) -> UdfRaw:
        return UdfRaw(id=self.normalize(row.id))


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


def test_mirror_methods_render_source_named_steps_and_constructor_inputs() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
        generated_code_options=("mirror_methods",),
    )

    ast.parse(text)
    assert "    def __init__(self, *, spark: SparkSession, ctx=None," in text
    assert "        orders: DataFrame," in text
    assert "    def normalize(self):" in text
    assert "    def add_customer(self):" in text
    assert "    def run(self) -> TransformResult:" in text
    assert "        self.orders = self._input_orders" in text
    assert "        self.normalize()" in text


def test_embed_exprs_render_static_helpers() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(EnrichOrders, generated_code_options=("embed_exprs",))),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
        generated_code_options=("embed_exprs",),
    )

    ast.parse(text)

    assert "@staticmethod\n    def clean_id(value):" in text
    assert "return F.lower(F.trim(value))" in text
    assert "self.clean_id(" in text


def test_embed_hooks_copies_raw_hook_source() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
        generated_code_options=("mirror_methods", "embed_hooks"),
    )

    ast.parse(text)

    assert "from testing.model.v1.orders.transforms.order import EnrichOrders" not in text
    assert "    def remove_negative_totals(self, *, orders, spark, ctx):" in text
    assert "return orders.where(F.col('net_total') >= 0)" in text


def test_embed_udfs_copies_udf_source() -> None:
    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(UdfPublished)),
        source_transform="tests.UdfPublished",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules={UdfRaw: "testing.model.v1.structure_generated.cache.pyspark.schemas.order"},
        generated_code_options=("mirror_methods", "embed_udfs"),
    )

    ast.parse(text)

    assert "self._impl" not in text
    assert "= F.udf(self.normalize, returnType=" in text
    assert "    @staticmethod\n    def normalize(value: Any):" in text
    assert "return value.strip().lower()" in text


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
