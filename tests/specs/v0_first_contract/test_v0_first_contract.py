import sys
from pathlib import Path
from typing import cast

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
MODEL = ROOT / "tests" / "model" / "v0" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(MODEL) not in sys.path:
    sys.path.insert(0, str(MODEL))

from structure.app.dsl.api import compile_transform
from structure.app.dsl.logic.model.Schema import DecimalType


def test_v0_fixture_imports_without_pyspark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    import orders.schemas.order  # type: ignore[import-not-found]
    import orders.transforms.order  # type: ignore[import-not-found]

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert orders.schemas.order.OrderRaw.__name__ == "OrderRaw"
    assert orders.transforms.order.NormalizeOrders.__name__ == "NormalizeOrders"


def test_v0_transform_compiles_to_first_contract_plan() -> None:
    from orders.schemas.order import OrderNormalized, OrderRaw  # type: ignore[import-not-found]
    from orders.transforms.order import NormalizeOrders  # type: ignore[import-not-found]

    plan = compile_transform(NormalizeOrders)

    assert plan.name == "NormalizeOrders"
    assert plan.output_schema is OrderNormalized
    assert [(item.name, item.schema, item.ordinal) for item in plan.inputs] == [("orders", OrderRaw, 0)]

    [step] = plan.steps
    assert step.name == "normalize"
    assert step.input_schema is OrderRaw
    assert step.output_schema is OrderNormalized
    assert step.ordinal == 0

    assert [predicate.kind for predicate in step.filters] == ["is_not_null", "is_not_null"]
    assert [predicate.args[0].data for predicate in step.filters] == [
        {"scope": "orders", "field": "id"},
        {"scope": "orders", "field": "customer_id"},
    ]
    assert [assignment.field.name for assignment in step.projection] == ["id", "customer_id", "total"]


def test_v0_total_projection_captures_decimal_coalesce() -> None:
    from orders.transforms.order import NormalizeOrders  # type: ignore[import-not-found]

    plan = compile_transform(NormalizeOrders)
    total = plan.steps[0].projection[2].expression

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


def test_transform_invocation_is_deferred_and_rejects_unknown_inputs() -> None:
    from orders.transforms.order import NormalizeOrders  # type: ignore[import-not-found]

    value = object()
    invocation = NormalizeOrders(orders=value)

    assert invocation._structure_bound_inputs == {"orders": value}

    try:
        NormalizeOrders(order=value)
    except TypeError as error:
        message = str(error)
    else:
        raise AssertionError("Unknown transform input should fail during deferred invocation construction")

    assert "unknown input" in message
    assert "orders" in message
