import pytest

from structure import String, Structure, StructureCompileError, Transform, field, input, output, transform
from structure.app.dsl.api import compile_transform


def test_field_access_produces_symbolic_projection_expressions(orders_plan) -> None:
    """I can have field access produce symbolic expressions."""

    normalize = orders_plan.steps[0]
    projection = {assignment.field.name: assignment.expression for assignment in normalize.projection}

    assert projection["tenant"].kind == "field"
    assert projection["tenant"].data == {"scope": "orders", "field": "tenant"}
    assert projection["tags"].kind == "field"
    assert projection["tags"].data == {"scope": "orders", "field": "tags"}


def test_dsl_functions_produce_nested_symbolic_expressions(orders_plan) -> None:
    """I can have DSL functions produce symbolic expressions."""

    projection = {assignment.field.name: assignment.expression for assignment in orders_plan.steps[0].projection}
    total = projection["total"]
    decimal_cast = total.args[0]

    assert total.kind == "call"
    assert total.data == {"function": "coalesce"}
    assert decimal_cast.kind == "call"
    assert decimal_cast.data == {"function": "to_decimal", "precision": 12, "scale": 2}
    assert decimal_cast.args[0].data == {"scope": "orders", "field": "total"}


def test_unsupported_python_control_flow_is_rejected() -> None:
    """I can have unsupported Python operations rejected."""

    class Raw(Structure):
        id = field(String(), nullable=False)

    class Published(Structure):
        id = field(String(), nullable=False)

    @transform
    class BadBoolean(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            if row.id:
                return Published(id=row.id)
            return Published(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadBoolean)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "unsupported symbolic code" in raised.value.diagnostic.problem_text()
