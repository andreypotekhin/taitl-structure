import sys

from structure.app.dsl.api import (
    Join,
    JoinHint,
    SchemaMode,
    String,
    Structure,
    Transform,
    compile_transform,
    field,
    input,
    output,
    transform,
)


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

    plan = compile_transform(EnrichOrders)

    assert plan.name == "EnrichOrders"
    assert plan.output_schema is OrderPublished
    assert plan.options == {"streaming_compatible": True}
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

    plan = compile_transform(EnrichOrders)

    assert [len(step.joins) for step in plan.steps] == [0, 1, 1, 1, 0]
    customer_join = plan.steps[1].joins[0]
    assert customer_join.input_name == "customer"
    assert customer_join.how is Join.LEFT
    assert customer_join.hint is JoinHint.BROADCAST
    assert customer_join.predicate.kind == "and"

    assert [hook.name for hook in plan.steps[0].before_hooks] == ["use_current_orders"]
    assert [hook.name for hook in plan.steps[0].after_hooks] == ["remove_negative_totals"]
    assert [hook.name for hook in plan.steps[3].after_hooks] == ["note_lookup_inputs"]
    assert [hook.name for hook in plan.steps[4].after_hooks] == ["add_quality_columns"]

    lookup_hook = plan.steps[3].after_hooks[0]
    assert lookup_hook.pass_inputs
    assert lookup_hook.schema_mode is SchemaMode.ALLOW_EXTRA_COLUMNS
    assert lookup_hook.project_output
    assert lookup_hook.streaming_safe

    quality_hook = plan.steps[4].after_hooks[0]
    assert quality_hook.project_output


def test_v1_symbolic_plan_records_expression_operators() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    plan = compile_transform(EnrichOrders)
    normalize = plan.steps[0]
    projection = {assignment.field.name: assignment.expression for assignment in normalize.projection}

    assert projection["net_total"].kind == "sub"
    assert [argument.kind for argument in projection["net_total"].args] == ["call", "call"]
    assert projection["is_large"].kind == "gt"
    assert projection["is_large"].args[1].kind == "literal"
    assert projection["is_large"].args[1].data == {"value": 1000}

    promotion_join = plan.steps[3].joins[0]
    assert promotion_join.predicate.kind == "and"
    assert promotion_join.predicate.args[1].kind == "null_safe_eq"


def test_transform_class_options_default_subtransform_options() -> None:
    """Class-level transform config options apply to every subtransform."""

    class Row(Structure):
        id = field(String(), nullable=False)

    @transform(target_backend="pyspark", target_platform="spark")
    class NormalizeRows(Transform):
        rows = input(Row)
        normalized = output(Row)

        def normalize(self, row: Row) -> Row:
            return Row(id=row.id)

        @transform(target_platform="polars")
        def publish(self, row: Row) -> Row:
            return Row(id=row.id)

    plan = compile_transform(NormalizeRows)

    assert plan.steps[0].options == {"target_backend": "pyspark", "target_platform": "spark"}
    assert plan.steps[1].options == {"target_backend": "pyspark", "target_platform": "polars"}
