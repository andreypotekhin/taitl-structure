import sys

from structure.app.dsl.api import compile_transform
from structure.app.dsl.logic.model.transforms.Join import Join
from structure.app.dsl.logic.model.transforms.JoinHint import JoinHint
from structure.app.dsl.logic.model.transforms.SchemaMode import SchemaMode
from structure.app.target.pyspark.api import lower_pyspark_plan


def test_v1_pyspark_recipe_lowering_is_spark_free() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    before = {name for name in sys.modules if name.startswith("pyspark")}

    recipe = lower_pyspark_plan(compile_transform(EnrichOrders))

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert recipe.transform == "EnrichOrders"
    assert recipe.backend.name == "pyspark"
    assert recipe.backend.target == ">=3.5,<4.1"


def test_v1_pyspark_recipe_preserves_inputs_and_steps() -> None:
    from testing.model.v1.orders.schemas.order import OrderPublished
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = lower_pyspark_plan(compile_transform(EnrichOrders))

    assert [(item.name, item.schema.__name__, item.ordinal, item.validation.reason) for item in recipe.inputs] == [
        ("orders", "OrderRaw", 0, "input"),
        ("customers", "Customer", 1, "input"),
        ("products", "Product", 2, "input"),
        ("promotions", "Promotion", 3, "input"),
    ]
    assert [(step.name, step.input_alias, step.output_alias) for step in recipe.steps] == [
        ("normalize", "order_raw", "order_normalized"),
        ("add_customer", "order_normalized", "order_with_customer"),
        ("add_product", "order_with_customer", "order_with_product"),
        ("add_promotion", "order_with_product", "order_with_promotion"),
        ("publish", "order_with_promotion", "order_published"),
    ]
    assert recipe.final_validation.schema is OrderPublished
    assert recipe.final_validation.reason == "final"


def test_v1_pyspark_recipe_records_joins_hooks_and_hook_inputs() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = lower_pyspark_plan(compile_transform(EnrichOrders))

    assert recipe.requires_hook_inputs
    assert [len(step.joins) for step in recipe.steps] == [0, 1, 1, 1, 0]

    customer_join = recipe.steps[1].joins[0]
    assert customer_join.input_name == "customers"
    assert customer_join.left_alias == "order_normalized"
    assert customer_join.right_alias == "customers"
    assert customer_join.how is Join.LEFT
    assert customer_join.hint is JoinHint.BROADCAST
    assert customer_join.predicate.kind == "and"

    assert [hook.name for hook in recipe.steps[0].before_hooks] == ["use_current_orders"]
    assert [hook.name for hook in recipe.steps[0].after_hooks] == ["remove_negative_totals"]
    assert [hook.name for hook in recipe.steps[3].after_hooks] == ["note_lookup_inputs"]
    assert [hook.name for hook in recipe.steps[4].after_hooks] == ["add_quality_columns"]


def test_v1_pyspark_recipe_records_expressions_and_projection_order() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = lower_pyspark_plan(compile_transform(EnrichOrders))
    normalize = recipe.steps[0]
    projection = {assignment.field.name: assignment.expression for assignment in normalize.projection}

    assert [assignment.field.name for assignment in normalize.projection] == [
        "tenant",
        "audit",
        "business",
        "id",
        "customer_id",
        "product_id",
        "promotion_code",
        "total",
        "discount",
        "net_total",
        "quantity",
        "tags",
        "attributes",
        "shipping",
        "is_large",
    ]
    assert projection["net_total"].kind == "sub"
    assert [argument.kind for argument in projection["net_total"].args] == ["call", "call"]
    assert projection["is_large"].kind == "gt"
    assert projection["is_large"].args[1].data == {"value": 1000}

    promotion_join = recipe.steps[3].joins[0]
    assert promotion_join.predicate.args[1].kind == "null_safe_eq"


def test_v1_pyspark_recipe_places_validation_boundaries() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = lower_pyspark_plan(compile_transform(EnrichOrders))

    assert [
        (validation.reason, validation.schema.__name__, validation.mode) for validation in recipe.steps[0].validations
    ] == [
        ("hook", "OrderNormalized", SchemaMode.STRICT),
        ("intermediate", "OrderNormalized", SchemaMode.STRICT),
    ]
    assert [(validation.reason, validation.mode, validation.project) for validation in recipe.steps[3].validations] == [
        ("hook", SchemaMode.ALLOW_EXTRA_COLUMNS, False),
        ("intermediate", SchemaMode.STRICT, False),
    ]
    assert [(validation.reason, validation.mode, validation.project) for validation in recipe.steps[4].validations] == [
        ("hook", SchemaMode.ALLOW_EXTRA_COLUMNS, True),
        ("hook_projected", SchemaMode.STRICT, False),
    ]
    assert recipe.final_validation.reason == "final"
    assert recipe.final_validation.mode is SchemaMode.STRICT
