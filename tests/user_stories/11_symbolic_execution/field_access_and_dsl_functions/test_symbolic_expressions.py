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

    class Raw(structure.Schema):
        promotion_code = structure.field(structure.String(), nullable=True, alias="promo-code")

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        id = structure.field(structure.String(), nullable=False)

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        customer_id = structure.field(structure.String(), nullable=False)
        status = structure.field(structure.String(), nullable=True)
        total = structure.field(structure.Integer(), nullable=False)
        tax = structure.field(structure.Integer(), nullable=False)
        price = structure.field(structure.Integer(), nullable=False)
        quantity = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        total = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        id = structure.field(structure.String(), nullable=False)

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        status = structure.field(structure.String(), nullable=False)

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        status = structure.field(structure.String(), nullable=True)

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        count = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=False)
        attributes = structure.field(
            structure.Map(structure.String(), structure.String(), value_contains_null=False), nullable=False
        )

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        status = structure.field(structure.String(), nullable=False)

    class Published(structure.Schema):
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


def test_scalar_casts_are_typed_symbolic_expressions() -> None:
    """I can cast a value without hiding its target type in a raw hook."""

    class Raw(structure.Schema):
        raw_count = structure.field(structure.String(), nullable=True)
        count = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
        count = structure.field(structure.Integer(), nullable=True)
        count_text = structure.field(structure.String(), nullable=False)
        try_count = structure.field(structure.Integer(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            source = cast(Any, row)
            return Published(
                count=source.raw_count.cast(structure.Integer()),
                count_text=source.count.astype(structure.String()),
                try_count=source.raw_count.try_cast(structure.Integer()),
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    actual = [
        (expression.kind, expression.type.name if expression.type else None, expression.nullable)
        for expression in projection.values()
    ]

    assert actual == [
        ("cast", "integer", True),
        ("cast", "string", False),
        ("try_cast", "integer", True),
    ]
    assert [expression.data for expression in projection.values()] == [
        {"spark_type": "int"},
        {"spark_type": "string"},
        {"spark_type": "int"},
    ]


def test_scalar_casts_require_structure_scalar_types() -> None:
    """I get a compile diagnostic for an opaque cast target."""

    class Raw(structure.Schema):
        raw_count = structure.field(structure.String(), nullable=True)

    class Published(structure.Schema):
        count = structure.field(structure.Integer(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(count=cast(Any, row).raw_count.cast("int"))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "cast(...) requires a scalar Structure type" in raised.value.diagnostic.problem_text()


def test_string_sql_helpers_are_typed_symbolic_expressions() -> None:
    """I can keep common string shaping and parsing visible to the compiler."""

    class Raw(structure.Schema):
        label = structure.field(structure.String(), nullable=True)

    class Published(structure.Schema):
        prefix = structure.field(structure.String(), nullable=True)
        parts = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)
        normalized = structure.field(structure.String(), nullable=True)
        extracted = structure.field(structure.String(), nullable=True)
        character_count = structure.field(structure.Integer(), nullable=True)
        title = structure.field(structure.String(), nullable=True)
        backward = structure.field(structure.String(), nullable=True)
        normalized_letters = structure.field(structure.String(), nullable=True)
        dash_position = structure.field(structure.Integer(), nullable=True)
        distance = structure.field(structure.Integer(), nullable=True)
        label = structure.field(structure.String(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                prefix=structure.substring(row.label, start=1, length=3),
                parts=structure.split(row.label, pattern="-"),
                normalized=structure.regexp_replace(row.label, pattern=r"\s+", replacement=" "),
                extracted=structure.regexp_extract(row.label, pattern=r"^([^-]+)", group=1),
                character_count=structure.length(row.label),
                title=structure.initcap(row.label),
                backward=structure.reverse(row.label),
                normalized_letters=structure.translate(row.label, matching="-", replacement="_"),
                dash_position=structure.instr(row.label, substring="-"),
                distance=structure.levenshtein(row.label, "release"),
                label=structure.concat_ws(" / ", row.label, "release"),
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [(expression.data, expression.type.name if expression.type else None) for expression in projection.values()] == [
        ({"function": "substring", "start": 1, "length": 3}, "string"),
        ({"function": "split", "pattern": "-", "limit": -1}, "array"),
        ({"function": "regexp_replace", "pattern": r"\s+", "replacement": " "}, "string"),
        ({"function": "regexp_extract", "pattern": r"^([^-]+)", "group": 1}, "string"),
        ({"function": "length"}, "integer"),
        ({"function": "initcap"}, "string"),
        ({"function": "reverse"}, "string"),
        ({"function": "translate", "matching": "-", "replacement": "_"}, "string"),
        ({"function": "instr", "substring": "-"}, "integer"),
        ({"function": "levenshtein"}, "integer"),
        ({"function": "concat_ws", "separator": " / "}, "string"),
    ]


def test_string_sql_helpers_reject_opaque_patterns_and_non_string_inputs() -> None:
    """I get compile diagnostics before an invalid SQL helper reaches Spark."""

    class Raw(structure.Schema):
        count = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
        value = structure.field(structure.String(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=structure.substring(row.count, start=1, length=3))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "substring(...) requires a String Structure expression" in raised.value.diagnostic.problem_text()


def test_concat_ws_requires_string_values() -> None:
    """I get a compile diagnostic before invalid concatenation reaches Spark."""

    class Raw(structure.Schema):
        count = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
        value = structure.field(structure.String(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=structure.concat_ws("-", row.count))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "concat_ws(...) requires a String Structure expression" in raised.value.diagnostic.problem_text()


def test_regexp_extract_requires_a_non_negative_group() -> None:
    """I get a compile diagnostic for an invalid capture-group index."""

    class Raw(structure.Schema):
        label = structure.field(structure.String(), nullable=False)

    class Published(structure.Schema):
        value = structure.field(structure.String(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=structure.regexp_extract(row.label, pattern=r"(.*)", group=-1))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "regexp_extract(...) group must be a non-negative integer" in raised.value.diagnostic.problem_text()


def test_temporal_sql_helpers_are_typed_symbolic_expressions() -> None:
    """I can derive dates and time buckets without a raw PySpark hook."""

    class Raw(structure.Schema):
        start_date = structure.field(structure.Date(), nullable=False)
        end_date = structure.field(structure.Date(), nullable=True)
        recorded_at = structure.field(structure.Timestamp(), nullable=True)

    class Published(structure.Schema):
        due_date = structure.field(structure.Date(), nullable=False)
        elapsed_days = structure.field(structure.Integer(), nullable=True)
        month = structure.field(structure.Timestamp(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                due_date=structure.date_add(row.start_date, days=7),
                elapsed_days=structure.datediff(row.end_date, row.start_date),
                month=structure.date_trunc(row.recorded_at, unit="month"),
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [(expression.data, expression.type.name if expression.type else None, expression.nullable) for expression in projection.values()] == [
        ({"function": "date_add", "days": 7}, "date", False),
        ({"function": "datediff"}, "integer", True),
        ({"function": "date_trunc", "unit": "month"}, "timestamp", True),
    ]


def test_temporal_sql_helpers_require_date_or_timestamp_inputs() -> None:
    """I get a compile diagnostic before an invalid temporal helper reaches Spark."""

    class Raw(structure.Schema):
        count = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
        due_date = structure.field(structure.Date(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(due_date=structure.date_add(row.count, days=1))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "date_add(...) requires a Date or Timestamp Structure expression" in raised.value.diagnostic.problem_text()


def test_numeric_sql_helpers_are_typed_symbolic_expressions() -> None:
    """I can apply deterministic numeric rounding without a raw PySpark hook."""

    class Raw(structure.Schema):
        amount = structure.field(structure.Decimal(precision=12, scale=2), nullable=True)

    class Published(structure.Schema):
        absolute_amount = structure.field(structure.Decimal(precision=12, scale=2), nullable=True)
        rounded_amount = structure.field(structure.Decimal(precision=12, scale=2), nullable=True)
        ceiling = structure.field(structure.Decimal(precision=11, scale=0), nullable=True)
        floor = structure.field(structure.Decimal(precision=11, scale=0), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                absolute_amount=structure.abs(row.amount),
                rounded_amount=structure.round(row.amount, scale=1),
                ceiling=structure.ceil(row.amount),
                floor=structure.floor(row.amount),
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [(expression.data, expression.type.name if expression.type else None) for expression in projection.values()] == [
        ({"function": "abs"}, "decimal"),
        ({"function": "round", "scale": 1}, "decimal"),
        ({"function": "ceil"}, "decimal"),
        ({"function": "floor"}, "decimal"),
    ]


def test_numeric_sql_helpers_require_numeric_inputs() -> None:
    """I get a compile diagnostic instead of a Spark type error for an invalid numeric helper."""

    class Raw(structure.Schema):
        label = structure.field(structure.String(), nullable=False)

    class Published(structure.Schema):
        value = structure.field(structure.Decimal(precision=11, scale=0), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=structure.ceil(row.label))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "ceil(...) requires a numeric Structure expression" in raised.value.diagnostic.problem_text()


def test_predicate_sql_helpers_are_typed_symbolic_expressions() -> None:
    """I can use function-style null and NaN checks in compiler-visible predicates."""

    class Raw(structure.Schema):
        label = structure.field(structure.String(), nullable=True)
        score = structure.field(structure.Double(), nullable=True)

    class Published(structure.Schema):
        missing_label = structure.field(structure.Boolean(), nullable=False)
        present_label = structure.field(structure.Boolean(), nullable=False)
        invalid_score = structure.field(structure.Boolean(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                missing_label=structure.isnull(row.label),
                present_label=structure.isnotnull(row.label),
                invalid_score=structure.isnan(row.score),
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [(expression.kind, expression.nullable) for expression in projection.values()] == [
        ("is_null", False),
        ("is_not_null", False),
        ("is_nan", False),
    ]


def test_isnan_requires_a_floating_point_expression() -> None:
    """I get a compile diagnostic when NaN cannot exist in the source type."""

    class Raw(structure.Schema):
        count = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
        invalid = structure.field(structure.Boolean(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(invalid=structure.isnan(row.count))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "isnan(...) requires a Float or Double Structure expression" in raised.value.diagnostic.problem_text()


def test_struct_get_field_is_a_typed_symbolic_expression() -> None:
    """I can read a Struct field by its declared name without a raw Column escape hatch."""

    class Address(structure.Schema):
        city = structure.field(structure.String(), nullable=False, alias="city-name")

    class Raw(structure.Schema):
        address = structure.field(structure.Struct(Address), nullable=True)

    class Published(structure.Schema):
        city = structure.field(structure.String(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(city=cast(Any, row).address.get_field("city"))

    expression = compile_transform(Publish).steps[0].projection[0].expression

    assert expression.kind == "get_field"
    assert expression.type == structure.String()
    assert expression.nullable
    assert expression.data == {"field": "city-name", "name": "city"}


def test_struct_get_field_rejects_unknown_fields() -> None:
    """I get a compiler diagnostic when a declared Struct does not contain the requested field."""

    class Address(structure.Schema):
        city = structure.field(structure.String(), nullable=False)

    class Raw(structure.Schema):
        address = structure.field(structure.Struct(Address), nullable=False)

    class Published(structure.Schema):
        city = structure.field(structure.String(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(city=cast(Any, row).address.get_field("postal_code"))

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "get_field(...) cannot find 'postal_code' in Address" in raised.value.diagnostic.problem_text()


def test_lookup_join_requires_boolean_expression() -> None:
    """Join predicates reject non-boolean expressions before target lowering."""

    class Raw(structure.Schema):
        id = structure.field(structure.String(), nullable=False)
        total = structure.field(structure.Integer(), nullable=False)

    class Lookup(structure.Schema):
        id = structure.field(structure.String(), nullable=False)

    class Published(structure.Schema):
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

    class Raw(structure.Schema):
        total = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
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
