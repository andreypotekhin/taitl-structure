from typing import Any, cast

import pytest

from structure import (
    Boolean,
    Integer,
    String,
    Structure,
    StructureCompileError,
    Transform,
    field,
    input,
    join_one,
    output,
    transform,
    trim,
    upper,
    when,
    where,
)
from structure.app.dsl.api import compile_transform


def test_field_access_produces_symbolic_projection_expressions(orders_plan) -> None:
    """I can have field access produce symbolic expressions."""

    normalize = orders_plan.steps[0]
    projection = {assignment.field.name: assignment.expression for assignment in normalize.projection}

    assert projection["tenant"].kind == "field"
    assert projection["tenant"].data == {
        "scope": "orders",
        "field": "tenant",
        "name": "tenant",
        "path": ("tenant",),
        "name_path": ("tenant",),
    }
    assert projection["tags"].kind == "field"
    assert projection["tags"].data == {
        "scope": "orders",
        "field": "tags",
        "name": "tags",
        "path": ("tags",),
        "name_path": ("tags",),
    }


def test_dsl_functions_produce_nested_symbolic_expressions(orders_plan) -> None:
    """I can have DSL functions produce symbolic expressions."""

    projection = {assignment.field.name: assignment.expression for assignment in orders_plan.steps[0].projection}
    total = projection["total"]
    decimal_cast = total.args[0]

    assert total.kind == "call"
    assert total.data == {"function": "coalesce"}
    assert decimal_cast.kind == "call"
    assert decimal_cast.data == {"function": "to_decimal", "precision": 12, "scale": 2}
    assert decimal_cast.args[0].data == {
        "scope": "orders",
        "field": "total",
        "name": "total",
        "path": ("total",),
        "name_path": ("total",),
    }


def test_alias_field_access_uses_spark_column_and_preserves_python_name() -> None:
    """Aliased fields keep Python names while referencing Spark columns."""

    class Raw(Structure):
        promotion_code = field(String(), nullable=True, alias="promo-code")

    class Published(Structure):
        promotion_code = field(String(), nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(promotion_code=row.promotion_code)

    plan = compile_transform(Publish)
    expression = plan.steps[0].projection[0].expression

    assert plan.steps[0].projection[0].field.column == "promotion_code"
    assert expression.data == {
        "scope": "rows",
        "field": "promo-code",
        "name": "promotion_code",
        "path": ("promo-code",),
        "name_path": ("promotion_code",),
    }


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


def test_plain_python_expression_extensions_are_symbolic() -> None:
    """I can use common Python expression forms for compiler-visible derived fields."""

    class Raw(Structure):
        customer_id = field(String(), nullable=False)
        total = field(Integer(), nullable=False)
        tax = field(Integer(), nullable=False)
        price = field(Integer(), nullable=False)
        quantity = field(Integer(), nullable=False)

    class Published(Structure):
        customer_id = field(String(), nullable=False)
        size_tier = field(String(), nullable=False)
        is_big = field(Boolean(), nullable=False)
        is_small = field(Boolean(), nullable=False)
        total_with_tax = field(Integer(), nullable=False)
        line_total = field(Integer(), nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            order = cast(Any, row)
            return Published(
                customer_id=upper(trim(order.customer_id)),
                size_tier=when(order.total >= 1000, "large").otherwise("standard"),
                is_big=order.total >= 1000,
                is_small=order.total < 100,
                total_with_tax=order.total + order.tax,
                line_total=order.price * order.quantity,
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert projection["customer_id"].data == {"function": "upper"}
    assert projection["customer_id"].args[0].data == {"function": "trim"}
    assert projection["size_tier"].kind == "when"
    assert projection["size_tier"].args[0].kind == "ge"
    assert projection["is_big"].kind == "ge"
    assert projection["is_small"].kind == "lt"
    assert projection["total_with_tax"].kind == "add"
    assert projection["line_total"].kind == "mul"


def test_where_requires_boolean_expression() -> None:
    """Filters reject non-boolean expressions before target lowering."""

    class Raw(Structure):
        total = field(Integer(), nullable=False)

    class Published(Structure):
        total = field(Integer(), nullable=False)

    @transform
    class BadFilter(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            where(row.total)
            return Published(total=row.total)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadFilter)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "where(...) requires a boolean Structure expression" in raised.value.diagnostic.problem_text()


def test_join_one_requires_boolean_expression() -> None:
    """Join predicates reject non-boolean expressions before target lowering."""

    class Raw(Structure):
        id = field(String(), nullable=False)
        total = field(Integer(), nullable=False)

    class Lookup(Structure):
        id = field(String(), nullable=False)

    class Published(Structure):
        id = field(String(), nullable=False)

    @transform
    class BadJoin(Transform):
        rows = input(Raw)
        lookups = input(Lookup)
        published = output(Published)

        def publish(self, row: Raw, lookup: Lookup) -> Published:
            join_one(lookup, on=row.total)
            return Published(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadJoin)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "join_one(on=...) requires a boolean Structure expression" in raised.value.diagnostic.problem_text()


def test_bare_when_requires_otherwise() -> None:
    """A conditional expression is complete only after otherwise(...)."""

    class Raw(Structure):
        total = field(Integer(), nullable=False)

    class Published(Structure):
        size_tier = field(String(), nullable=False)

    @transform
    class BadWhen(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            order = cast(Any, row)
            return Published(size_tier=when(order.total >= 1000, "large"))

    with pytest.raises(TypeError, match=r"when\(\.\.\.\) must end with \.otherwise\(\.\.\.\)"):
        compile_transform(BadWhen)
