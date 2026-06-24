import sys
from typing import cast

from structure.app.dsl.api import DecimalType, compile_transform


def test_v1_fixture_imports_without_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    import testing.model.v1.orders.schemas.order
    import testing.model.v1.orders.transforms.order

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert testing.model.v1.orders.schemas.order.OrderRaw.__name__ == "OrderRaw"
    assert testing.model.v1.orders.transforms.order.EnrichOrders.__name__ == "EnrichOrders"


def test_v1_first_slice_compiles_to_normalization_plan() -> None:
    from testing.model.v1.orders.schemas.customer import Customer
    from testing.model.v1.orders.schemas.order import OrderNormalized, OrderRaw
    from testing.model.v1.orders.schemas.product import Product
    from testing.model.v1.orders.schemas.promotion import Promotion
    from testing.model.v1.orders.transforms.order import EnrichOrders

    plan = compile_transform(EnrichOrders)

    assert plan.name == "EnrichOrders"
    assert [(item.name, item.schema, item.ordinal) for item in plan.inputs] == [
        ("orders", OrderRaw, 0),
        ("customers", Customer, 1),
        ("products", Product, 2),
        ("promotions", Promotion, 3),
    ]

    step = plan.steps[0]
    assert step.name == "normalize"
    assert step.input_schema is OrderRaw
    assert step.output_schema is OrderNormalized
    assert step.ordinal == 0

    assert [predicate.kind for predicate in step.filters] == ["is_not_null", "is_not_null", "is_not_null"]
    assert [predicate.args[0].data for predicate in step.filters] == [
        {"scope": "orders", "field": "id"},
        {"scope": "orders", "field": "customer_id"},
        {"scope": "orders", "field": "product_id"},
    ]
    assert [assignment.field.name for assignment in step.projection][:8] == [
        "tenant",
        "audit",
        "business",
        "id",
        "customer_id",
        "product_id",
        "promotion_code",
        "total",
    ]


def test_v1_first_slice_total_projection_captures_decimal_coalesce() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    plan = compile_transform(EnrichOrders)
    projection = {assignment.field.name: assignment.expression for assignment in plan.steps[0].projection}
    total = projection["total"]

    assert total.kind == "call"
    data = cast(dict[str, object], total.data)
    assert data["function"] == "coalesce"
    assert isinstance(total.type, DecimalType)
    assert total.type.precision == 12
    assert total.type.scale == 2

    decimal_cast, fallback = total.args
    assert decimal_cast.kind == "call"
    assert decimal_cast.data == {"function": "to_decimal", "precision": 12, "scale": 2}
    assert decimal_cast.args[0].data == {"scope": "orders", "field": "total"}
    assert fallback.kind == "literal"
    assert fallback.data == {"value": 0}


def test_v1_transform_invocation_is_deferred_and_rejects_unknown_inputs() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    value = object()
    invocation = EnrichOrders(
        orders=value,
        customers=value,
        products=value,
        promotions=value,
    )

    assert invocation._structure_bound_inputs == {
        "orders": value,
        "customers": value,
        "products": value,
        "promotions": value,
    }

    try:
        EnrichOrders(order=value)
    except TypeError as error:
        message = str(error)
    else:
        raise AssertionError("Unknown transform input should fail during deferred invocation construction")

    assert "unknown input" in message
    assert "orders" in message
