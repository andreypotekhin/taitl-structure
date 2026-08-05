import ast
import builtins
import sys
from typing import Any, cast

import pytest

from structure import *
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.logic.HardWrapGeneratedPython import HardWrapGeneratedPython
from structure.plugin.pyspark.render.logic.RenderEmbeddedHooks import EmbeddedHookError


def _recipe(transform) -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(transform, materialize_schemas=False).lowered,
    )


class CacheRaw(Schema):
    id = string(nullable=False)
    status = string(nullable=True)


class CachePublished(Schema):
    id = string(nullable=False)
    status = string(nullable=True)


class ExplicitStorageLevel:
    useDisk = True
    useMemory = True
    useOffHeap = False
    deserialized = False
    replication = 2


class UdfRaw(Schema):
    id = string(nullable=False)


@transform
class UdfPublished(Transform):
    rows = input(UdfRaw)
    published = output(UdfRaw)

    @special(type="udf", return_type=types.string(), nullable=False)
    def normalize(value: Any):
        return value.strip().lower()

    def publish(self, row: UdfRaw) -> UdfRaw:
        return UdfRaw(id=self.normalize(row.id))


class UdfPipeline(Transform):
    rows = input(UdfRaw)

    pipeline = UdfPublished(rows=rows).to()


@transform
class CachePublishedOrders(Transform):
    orders = input(CacheRaw)
    published = output(CachePublished)

    @step(cache=True)
    def publish(self, order: CacheRaw) -> CachePublished:
        return CachePublished(id=order.id, status=order.status)


@transform
class ExplicitlyCachedPublishedOrders(Transform):
    orders = input(CacheRaw)
    published = output(CachePublished)

    @step(cache=ExplicitStorageLevel())
    def publish(self, order: CacheRaw) -> CachePublished:
        return CachePublished(id=order.id, status=order.status)


EMBEDDED_HOOK_GLOBAL = "source-module-state"


@transform
class EmbeddedHookWithState(Transform):
    rows = input(CacheRaw)
    published = output(CachePublished)
    state: object

    def publish(self, row: CacheRaw) -> CachePublished:
        return CachePublished(id=row.id, status=row.status)

    @raw(inout=lane(rows) | lane(rows))
    def publish_hook(self, *, rows, spark, ctx):
        return self.state


@transform
class EmbeddedHookWithGlobal(Transform):
    rows = input(CacheRaw)
    published = output(CachePublished)

    def publish(self, row: CacheRaw) -> CachePublished:
        return CachePublished(id=row.id, status=row.status)

    @raw(inout=lane(rows) | lane(rows))
    def publish_hook(self, *, rows, spark, ctx):
        return EMBEDDED_HOOK_GLOBAL


@transform
class EmbeddedHookWithSuper(Transform):
    rows = input(CacheRaw)
    published = output(CachePublished)

    def publish(self, row: CacheRaw) -> CachePublished:
        return CachePublished(id=row.id, status=row.status)

    @raw(inout=lane(rows) | lane(rows))
    def publish_hook(self, *, rows, spark, ctx):
        return super().publish_hook(rows=rows, spark=spark, ctx=ctx)  # type: ignore[misc]


def test_v1_transform_module_renderer_is_spark_free() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    before = {name for name in sys.modules if name.startswith("pyspark")}

    text = PySpark.render.transform()(
        _recipe(EnrichOrders),
        source_transform="testing.model.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
    )

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert text.startswith("from __future__ import annotations\nfrom pyspark.sql import DataFrame, SparkSession\n")


def test_v1_transform_module_renderer_renders_class_runtime_shape() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        _recipe(EnrichOrders),
        source_transform="testing.model.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
    )

    assert "from testing.model.orders.transforms.order import EnrichOrders" in text
    assert (
        "from testing.model.structure_generated.runtime.schema_assert import "
        "TransformResult, assert_schema, project_schema" in text
    )
    assert "class EnrichOrdersGenerated:" in text
    assert "        self._impl = EnrichOrders()" in text
    assert "        orders: DataFrame," in text
    assert '        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")' in text
    assert "HookInputs" not in text


def test_v1_transform_module_renderer_composes_steps_and_final_return() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        _recipe(EnrichOrders),
        source_transform="testing.model.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
    )

    assert "        # Step method: normalize" in text
    assert "        # Step method: add_customer" in text
    assert "        # Step method: add_product" in text
    assert "        # Step method: add_promotion" in text
    assert "        # Step method: publish" in text
    assert (
        "        orders = self._impl.use_current_orders(orders=_input_orders, spark=self.spark, ctx=self.ctx)" in text
    )
    assert "orders =" in text
    assert "self._impl.note_lookup_inputs(" in text
    assert "customers=_input_customers" in text
    assert "products=_input_products" in text
    assert "        published = project_schema(published, ORDER_PUBLISHED_SCHEMA)" in text
    assert text.count('assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")') == 2
    assert text.rstrip().endswith(
        '        return TransformResult({"published": published}, single=True, '
        'schema={"published": ORDER_PUBLISHED_SCHEMA})'
    )


def test_composed_transform_qualifies_nested_special_udf_owner() -> None:
    text = PySpark.render.transform()(
        _recipe(UdfPipeline),
        source_transform=f"{__name__}.UdfPipeline",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules={UdfRaw: "testing.model.structure_generated.cache.pyspark.schemas.order"},
    )

    ast.parse(text)
    assert "UdfPublished()" in text
    assert "self._impl_udf_" in text
    assert ".normalize" in text


def test_mirror_methods_render_source_named_steps_and_constructor_inputs() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        _recipe(EnrichOrders),
        source_transform="testing.model.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
        generated_code_options=("mirror_methods",),
    )

    ast.parse(text)
    assert "    def __init__(" in text
    assert "        spark: SparkSession," in text
    assert "        orders: DataFrame," in text
    assert "    def normalize(self):" in text
    assert "    def add_customer(self):" in text
    assert "    def run(self) -> TransformResult:" in text
    assert "        self.orders = self._input_orders" in text
    assert "        self.normalize()" in text


def test_embed_exprs_render_static_helpers() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        cast(
            PySparkExecutionPlan,
            Compiler.frontend.compile()(
                EnrichOrders,
                materialize_schemas=False,
                generated_code_options=("embed_exprs",),
            ).lowered,
        ),
        source_transform="testing.model.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
        generated_code_options=("embed_exprs",),
    )

    ast.parse(text)

    assert "@staticmethod\n    def clean_id(value):" in text
    assert "return F.lower(F.trim(value))" in text
    assert "self.clean_id(" in text


def test_embed_hooks_copies_raw_hook_source() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        _recipe(EnrichOrders),
        source_transform="testing.model.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
        generated_code_options=("mirror_methods", "embed_hooks"),
    )

    ast.parse(text)

    assert "from testing.model.orders.transforms.order import EnrichOrders" not in text
    assert "self._impl" not in text
    assert "orders = self.remove_negative_totals(orders=orders, spark=self.spark, ctx=self.ctx)" in text
    assert "    def remove_negative_totals(self, *, orders, spark, ctx):" in text
    assert "return orders.where(F.col('net_total') >= 0)" in text
    assert text.index("    def run(self) -> TransformResult:") < text.index("    def remove_negative_totals(")


@pytest.mark.parametrize(
    ("transform", "reference"),
    (
        (EmbeddedHookWithState, "self.state"),
        (EmbeddedHookWithGlobal, "EMBEDDED_HOOK_GLOBAL"),
        (EmbeddedHookWithSuper, "super()"),
    ),
)
def test_embed_hooks_rejects_source_state_dependencies(transform, reference: str) -> None:
    with pytest.raises(EmbeddedHookError) as raised:
        _render(transform, generated_code_options=("embed_hooks",))

    assert raised.value.diagnostic.code == "GEN-E0903"
    assert raised.value.diagnostic.context["hook"] == "publish_hook"
    assert reference in raised.value.diagnostic.problem


def test_embed_hooks_rejects_python_udf_without_embed_udfs() -> None:
    with pytest.raises(EmbeddedHookError) as raised:
        _render(UdfPublished, generated_code_options=("embed_hooks",))

    assert raised.value.diagnostic.code == "GEN-E0903"
    assert "Python UDFs" in raised.value.diagnostic.problem


def test_embed_hooks_rejects_closure_dependencies() -> None:
    captured = "source closure"

    @transform
    class EmbeddedHookWithClosure(Transform):
        rows = input(CacheRaw)
        published = output(CachePublished)

        def publish(self, row: CacheRaw) -> CachePublished:
            return CachePublished(id=row.id, status=row.status)

        @raw(inout=lane(rows) | lane(rows))
        def publish_hook(self, *, rows, spark, ctx):
            return captured

    with pytest.raises(EmbeddedHookError) as raised:
        _render(EmbeddedHookWithClosure, generated_code_options=("embed_hooks",))

    assert raised.value.diagnostic.context["hook"] == "publish_hook"
    assert "captured" in raised.value.diagnostic.problem


def test_embed_udfs_copies_udf_source() -> None:
    text = PySpark.render.transform()(
        _recipe(UdfPublished),
        source_transform="tests.UdfPublished",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules={UdfRaw: "testing.model.structure_generated.cache.pyspark.schemas.order"},
        generated_code_options=("mirror_methods", "embed_udfs"),
    )

    ast.parse(text)

    assert "from __future__ import annotations" in text
    assert "self._impl" not in text
    assert ".udf(" in text
    assert "self.normalize" in text
    assert "returnType=" in text
    assert "    @staticmethod\n    def normalize(value: Any):" in text
    assert "return value.strip().lower()" in text


def test_v2_cache_directive_renders_as_post_projection_persist() -> None:
    text = PySpark.render.transform()(
        _recipe(CachePublishedOrders),
        source_transform="tests.CachePublishedOrders",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules={
            CacheRaw: "testing.model.structure_generated.cache.pyspark.schemas.order",
            CachePublished: "testing.model.structure_generated.cache.pyspark.schemas.order",
        },
    )

    assert "        orders = orders.select(" in text
    assert "        orders = orders.persist()" in text
    assert text.index("        orders = orders.select(") < text.index("        orders = orders.persist()")
    assert text.index("        orders = orders.persist()") < text.index("        # Step method: published")


def test_v2_cache_directive_is_visible_in_explain_output() -> None:
    text = render_explain_report(CachePublishedOrders)

    assert "operations: cache(row_preserving)" in text


def test_v2_cache_directive_preserves_an_explicit_storage_level() -> None:
    recipe = _recipe(ExplicitlyCachedPublishedOrders)
    operation = recipe.steps[0].operations[0]
    text = PySpark.render.transform()(
        recipe,
        source_transform="tests.ExplicitlyCachedPublishedOrders",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules={
            CacheRaw: "testing.model.structure_generated.cache.pyspark.schemas.order",
            CachePublished: "testing.model.structure_generated.cache.pyspark.schemas.order",
        },
    )

    assert operation.cache is not None
    assert operation.cache.storage_level == (True, True, False, False, 2)
    assert "from pyspark import StorageLevel" in text
    assert "        orders = orders.persist(StorageLevel(True, True, False, False, 2))" in text


def test_transform_module_hard_wrap_keeps_generated_python_parseable() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    text = PySpark.render.transform()(
        _recipe(EnrichOrders),
        source_transform="testing.model.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules=_schema_modules(),
        generated_code_hard_wrap=88,
    )

    ast.parse(text)
    assert builtins.max(len(line) for line in text.splitlines()) <= 88


def test_transform_module_hard_wrap_preserves_long_fingerprint_literals() -> None:
    text = HardWrapGeneratedPython()(
        "STRUCTURE_ARTIFACT_FINGERPRINTS = {"
        "'integration.pyspark.v6.test_ordered_timeline_scan.FibonacciFromTimeline':"
        f"'{('abcdef0123456789' * 8)}'"
        "}\n",
        width=88,
    )

    ast.parse(text)


def _schema_modules() -> dict[type, str]:
    from testing.model.orders.schemas.customer import Customer
    from testing.model.orders.schemas.order import (
        OrderNormalized,
        OrderPublished,
        OrderRaw,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
    )
    from testing.model.orders.schemas.product import Product
    from testing.model.orders.schemas.promotion import Promotion

    order_module = "testing.model.structure_generated.orders.pyspark.schemas.order"
    return {
        OrderRaw: order_module,
        OrderNormalized: order_module,
        OrderWithCustomer: order_module,
        OrderWithProduct: order_module,
        OrderWithPromotion: order_module,
        OrderPublished: order_module,
        Customer: "testing.model.structure_generated.orders.pyspark.schemas.customer",
        Product: "testing.model.structure_generated.orders.pyspark.schemas.product",
        Promotion: "testing.model.structure_generated.orders.pyspark.schemas.promotion",
    }


def _render(transform: type[Transform], *, generated_code_options: tuple[str, ...]) -> str:
    return PySpark.render.transform()(
        _recipe(transform),
        source_transform=f"{transform.__module__}.{transform.__name__}",
        runtime_module="testing.model.structure_generated.runtime.schema_assert",
        schema_modules={CacheRaw: "testing.cache", CachePublished: "testing.cache", UdfRaw: "testing.cache"},
        generated_code_options=generated_code_options,
    )
