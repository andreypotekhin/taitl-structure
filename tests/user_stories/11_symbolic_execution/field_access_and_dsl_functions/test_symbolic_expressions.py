from typing import Any, cast

import pytest

import structure
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

    class Raw(structure.Structure):
        promotion_code = structure.field(structure.String(), nullable=True, alias="promo-code")

    class Published(structure.Structure):
        promotion_code = structure.field(structure.String(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

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

    class Raw(structure.Structure):
        id = structure.field(structure.String(), nullable=False)

    class Published(structure.Structure):
        id = structure.field(structure.String(), nullable=False)

    @structure.transform
    class BadBoolean(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            if row.id:
                return Published(id=row.id)
            return Published(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadBoolean)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "unsupported symbolic code" in raised.value.diagnostic.problem_text()


def test_plain_python_expression_extensions_are_symbolic() -> None:
    """I can use common Python expression forms for compiler-visible derived fields."""

    class Raw(structure.Structure):
        customer_id = structure.field(structure.String(), nullable=False)
        status = structure.field(structure.String(), nullable=True)
        total = structure.field(structure.Integer(), nullable=False)
        tax = structure.field(structure.Integer(), nullable=False)
        price = structure.field(structure.Integer(), nullable=False)
        quantity = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Structure):
        customer_id = structure.field(structure.String(), nullable=False)
        size_tier = structure.field(structure.String(), nullable=False)
        is_big = structure.field(structure.Boolean(), nullable=False)
        is_medium = structure.field(structure.Boolean(), nullable=False)
        is_open = structure.field(structure.Boolean(), nullable=True)
        is_small = structure.field(structure.Boolean(), nullable=False)
        total_with_tax = structure.field(structure.Integer(), nullable=False)
        line_total = structure.field(structure.Integer(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            order = cast(Any, row)
            return Published(
                customer_id=structure.upper(structure.trim(order.customer_id)),
                size_tier=structure.when(order.total >= 1000, "large").otherwise("standard"),
                is_big=order.total >= 1000,
                is_medium=order.total.between(100, 999),
                is_open=order.status.isin("new", "held"),
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
    assert projection["is_medium"].kind == "and"
    assert projection["is_medium"].args[0].kind == "ge"
    assert projection["is_medium"].args[1].kind == "le"
    assert projection["is_open"].kind == "isin"
    assert [argument.kind for argument in projection["is_open"].args] == ["field", "literal", "literal"]
    assert projection["is_small"].kind == "lt"
    assert projection["total_with_tax"].kind == "add"
    assert projection["line_total"].kind == "mul"


def test_where_requires_boolean_expression() -> None:
    """Filters reject non-boolean expressions before target lowering."""

    class Raw(structure.Structure):
        total = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Structure):
        total = structure.field(structure.Integer(), nullable=False)

    @structure.transform
    class BadFilter(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            structure.where(row.total)
            return Published(total=row.total)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadFilter)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "where(...) requires a boolean Structure expression" in raised.value.diagnostic.problem_text()


def test_variadic_where_records_the_same_order_as_serial_where_calls() -> None:
    """I can pass serial filter predicates to one where(...) call."""

    class Raw(structure.Structure):
        id = structure.field(structure.String(), nullable=False)

    class Published(structure.Structure):
        id = structure.field(structure.String(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            structure.where(row.id.is_not_null(), row.id == "accepted")  # type: ignore[attr-defined]
            return Published(id=row.id)

    operations = compile_transform(Publish).steps[0].operations

    assert [operation.kind for operation in operations] == ["filter", "filter"]
    assert [operation.filter.kind for operation in operations if operation.filter is not None] == ["is_not_null", "eq"]


def test_membership_predicates_require_values() -> None:
    """Membership predicates need at least one candidate value."""

    class Raw(structure.Structure):
        status = structure.field(structure.String(), nullable=False)

    class Published(structure.Structure):
        known = structure.field(structure.Boolean(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(known=cast(Any, row).status.isin())

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "isin(...) requires at least one value" in raised.value.diagnostic.problem_text()


def test_string_predicates_are_typed_symbolic_expressions() -> None:
    """I can express string matching without a raw SQL expression."""

    class Raw(structure.Structure):
        status = structure.field(structure.String(), nullable=True)

    class Published(structure.Structure):
        has_new = structure.field(structure.Boolean(), nullable=True)
        is_new = structure.field(structure.Boolean(), nullable=True)
        is_new_case_insensitive = structure.field(structure.Boolean(), nullable=True)
        has_release_number = structure.field(structure.Boolean(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            status = cast(Any, row).status
            return Published(
                has_new=status.contains("new"),
                is_new=status.like("new%"),
                is_new_case_insensitive=status.ilike("NEW%"),
                has_release_number=status.rlike(r"release-[0-9]+"),
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [(expression.kind, expression.data, expression.nullable) for expression in projection.values()] == [
        ("contains", {"pattern": "new"}, True),
        ("like", {"pattern": "new%"}, True),
        ("ilike", {"pattern": "NEW%"}, True),
        ("rlike", {"pattern": r"release-[0-9]+"}, True),
    ]


def test_string_predicates_require_string_expressions() -> None:
    """I get a compile diagnostic instead of a Spark type error for invalid string matching."""

    class Raw(structure.Structure):
        count = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Structure):
        matched = structure.field(structure.Boolean(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(matched=cast(Any, row).count.contains("1"))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "contains(...) requires a String Structure expression" in raised.value.diagnostic.problem_text()


def test_collection_indexing_is_typed_and_symbolic() -> None:
    """I can read an array item or map value without dropping into a raw hook."""

    class Raw(structure.Structure):
        tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=False)
        attributes = structure.field(
            structure.Map(structure.String(), structure.String(), value_contains_null=False), nullable=False
        )

    class Published(structure.Structure):
        first_tag = structure.field(structure.String(), nullable=True)
        region = structure.field(structure.String(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            source = cast(Any, row)
            return Published(first_tag=source.tags[0], region=source.attributes["region"])

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [(expression.kind, expression.type, expression.nullable) for expression in projection.values()] == [
        ("item", structure.String(), True),
        ("item", structure.String(), True),
    ]
    assert [expression.args[1].data for expression in projection.values()] == [{"value": 0}, {"value": "region"}]


def test_collection_indexing_requires_a_matching_collection_and_key_type() -> None:
    """I get compile diagnostics for invalid collection indexing instead of a Spark runtime error."""

    class Raw(structure.Structure):
        status = structure.field(structure.String(), nullable=False)

    class Published(structure.Structure):
        value = structure.field(structure.String(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=cast(Any, row).status[0])

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "Indexing requires an Array or Map Structure expression" in raised.value.diagnostic.problem_text()


def test_lookup_join_requires_boolean_expression() -> None:
    """Join predicates reject non-boolean expressions before target lowering."""

    class Raw(structure.Structure):
        id = structure.field(structure.String(), nullable=False)
        total = structure.field(structure.Integer(), nullable=False)

    class Lookup(structure.Structure):
        id = structure.field(structure.String(), nullable=False)

    class Published(structure.Structure):
        id = structure.field(structure.String(), nullable=False)

    @structure.transform
    class BadJoin(structure.Transform):
        rows = structure.input(Raw)
        lookups = structure.input(Lookup)
        published = structure.output(Published)

        def publish(self, row: Raw, lookup: Lookup) -> Published:
            structure.lookup_join(lookup, on=row.total)
            return Published(id=row.id)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(BadJoin)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "lookup_join(on=...) requires a boolean Structure expression" in raised.value.diagnostic.problem_text()


def test_bare_when_requires_otherwise() -> None:
    """A conditional expression is complete only after otherwise(...)."""

    class Raw(structure.Structure):
        total = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Structure):
        size_tier = structure.field(structure.String(), nullable=False)

    @structure.transform
    class BadWhen(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            order = cast(Any, row)
            return Published(size_tier=structure.when(order.total >= 1000, "large"))

    with pytest.raises(TypeError, match=r"when\(\.\.\.\) must end with \.otherwise\(\.\.\.\)"):
        compile_transform(BadWhen)
