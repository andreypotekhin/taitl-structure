import sys
from typing import cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl.types import StructType
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def _analysis(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False).analysis


def test_v1_fixture_imports_without_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    import testing.model.v1.orders.transforms.order

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert testing.model.v1.orders.transforms.order.EnrichOrders.__name__ == "EnrichOrders"


def test_v1_transform_compiles_to_ordered_symbolic_plan() -> None:
    from testing.model.v1.orders.schemas.customer import Customer
    from testing.model.v1.orders.schemas.order import OrderPublished, OrderRaw
    from testing.model.v1.orders.schemas.product import Product
    from testing.model.v1.orders.schemas.promotion import Promotion
    from testing.model.v1.orders.transforms.order import EnrichOrders

    plan = _analysis(EnrichOrders)

    assert plan.name == "EnrichOrders"
    assert plan.output_schema is OrderPublished
    assert plan.options == {"streaming": True}
    assert [(item.name, item.schema, item.ordinal) for item in plan.inputs] == [
        ("orders", OrderRaw, 0),
        ("customers", Customer, 1),
        ("products", Product, 2),
        ("promotions", Promotion, 3),
    ]
    assert [step.name for step in plan.steps] == [
        "normalize",
        "add_customer",
        "add_product",
        "add_promotion",
        "publish",
    ]


def test_v1_symbolic_plan_records_joins_and_hooks() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    plan = _analysis(EnrichOrders)

    bodies = tuple(cast(PySparkStepBody, step.plugin_body) for step in plan.steps)
    assert [len(body.joins) for body in bodies] == [0, 1, 1, 1, 0]
    customer_join = bodies[1].joins[0]
    assert customer_join.input_name == "customer"
    assert customer_join.how is Join.LEFT
    assert customer_join.hint is JoinHint.BROADCAST
    assert customer_join.predicate.kind == "and"

    assert [hook.name for hook in plan.steps[0].before_hooks] == ["use_current_orders"]
    assert [hook.name for hook in plan.steps[0].after_hooks] == ["remove_negative_totals"]
    assert [hook.name for hook in plan.steps[3].after_hooks] == ["note_lookup_inputs"]
    assert [hook.name for hook in plan.steps[4].after_hooks] == ["add_quality_columns"]

    lookup_hook = plan.steps[3].after_hooks[0]
    assert lookup_hook.sources == ("orders", "input:customers", "input:products")
    assert lookup_hook.schema_mode is SchemaMode.ALLOW_EXTRA_COLUMNS
    assert lookup_hook.project_output
    assert lookup_hook.streaming_safe

    quality_hook = plan.steps[4].after_hooks[0]
    assert quality_hook.project_output


def test_v1_symbolic_plan_records_expression_operators() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    plan = _analysis(EnrichOrders)
    normalize = plan.steps[0]
    projection = {
        assignment.field.name: assignment.expression
        for assignment in cast(PySparkStepBody, normalize.plugin_body).projection
    }

    assert projection["net_total"].kind == "cast"
    assert [argument.kind for argument in projection["net_total"].args[0].args] == ["call", "call"]
    assert projection["is_large"].kind == "gt"
    assert projection["is_large"].args[1].kind == "literal"
    assert projection["is_large"].args[1].data == {"value": 1000}

    promotion_join = cast(PySparkStepBody, plan.steps[3].plugin_body).joins[0]
    assert promotion_join.predicate.kind == "and"
    assert promotion_join.predicate.args[1].kind == "null_safe_eq"


def test_v1_symbolic_plan_records_nested_struct_construction() -> None:
    class Address(Schema):
        city = string(nullable=False)
        postal_code = string(nullable=False)

    class Raw(Schema):
        id = string(nullable=False)
        shipping = struct(Address, nullable=True)

    class Published(Schema):
        id = string(nullable=False)
        shipping = struct(Address, nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            where(row.shipping.is_not_null())  # type: ignore[attr-defined]
            return Published(
                id=row.id,
                shipping=Address(
                    city=trim(row.shipping.city),  # type: ignore[attr-defined]
                    postal_code=row.shipping.postal_code,  # type: ignore[attr-defined]
                ),
            )

    plan = _analysis(Publish)
    projection = {
        assignment.field.name: assignment.expression
        for assignment in cast(PySparkStepBody, plan.steps[0].plugin_body).projection
    }
    shipping = projection["shipping"]

    assert shipping.kind == "struct"
    assert cast(StructType, shipping.type).schema is Address
    assert shipping.nullable is False
    assert [argument.kind for argument in shipping.args] == ["call", "field"]


def test_v1_symbolic_plan_rejects_incompatible_nested_struct_assignment() -> None:
    class Address(Schema):
        city = string(nullable=False)
        postal_code = string(nullable=False)

    class TenantKey(Schema):
        tenant_id = string(nullable=False)

    class Raw(Schema):
        id = string(nullable=False)

    class Published(Schema):
        tenant = struct(TenantKey, nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(tenant=Address(city=row.id, postal_code=row.id))

    with pytest.raises(Exception, match=r"expects Struct\(TenantKey\).*Struct\(Address\)"):
        _analysis(Publish)


def test_transform_class_options_default_step_method_options() -> None:
    """Class-level transform config options apply to every step method."""

    class Row(Schema):
        id = string(nullable=False)

    @transform(target_backend="pyspark", target_platform="spark")
    class NormalizeRows(Transform):
        rows = input(Row)
        normalized = output(Row)

        def normalize(self, row: Row) -> Row:
            return Row(id=row.id)

        @step(target_platform="polars")
        def publish(self, row: Row) -> Row:
            return Row(id=row.id)

    plan = _analysis(NormalizeRows)

    assert plan.steps[0].options == {"target_backend": "pyspark", "target_platform": "spark"}
    assert plan.steps[1].options == {"target_backend": "pyspark", "target_platform": "polars"}
