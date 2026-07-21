from typing import Any, cast

import pytest

from structure import *
from structure.plugin.pyspark import *
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def test_field_access_produces_symbolic_projection_expressions(orders_plan) -> None:
    """I can have field access produce symbolic expressions."""

    normalize = orders_plan.steps[0]
    projection = {
        assignment.field.name: assignment.expression
        for assignment in cast(PySparkStepBody, normalize.plugin_body).projection
    }

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

    projection = {
        assignment.field.name: assignment.expression
        for assignment in cast(PySparkStepBody, orders_plan.steps[0].plugin_body).projection
    }
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

    class Raw(Schema):
        promotion_code = string(nullable=True, alias='promo-code')

    class Published(Schema):
        promotion_code = string(nullable=True)

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

    class Raw(Schema):
        id = string(nullable=False)

    class Published(Schema):
        id = string(nullable=False)

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

    class Raw(Schema):
        customer_id = string(nullable=False)
        status = string(nullable=True)
        total = integer(nullable=False)
        tax = integer(nullable=False)
        price = integer(nullable=False)
        quantity = integer(nullable=False)

    class Published(Schema):
        customer_id = string(nullable=False)
        size_tier = string(nullable=False)
        is_big = boolean(nullable=False)
        is_medium = boolean(nullable=False)
        is_open = boolean(nullable=True)
        is_small = boolean(nullable=False)
        total_with_tax = integer(nullable=False)
        line_total = integer(nullable=False)

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

    class Raw(Schema):
        total = integer(nullable=False)

    class Published(Schema):
        total = integer(nullable=False)

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


def test_variadic_where_records_the_same_order_as_serial_where_calls() -> None:
    """I can pass serial filter predicates to one where(...) call."""

    class Raw(Schema):
        id = string(nullable=False)

    class Published(Schema):
        id = string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            where(row.id.is_not_null(), row.id == "accepted")  # type: ignore[attr-defined]
            return Published(id=row.id)

    operations = compile_transform(Publish).steps[0].operations

    assert [operation.kind for operation in operations] == ["filter", "filter"]
    assert [operation.filter.kind for operation in operations if operation.filter is not None] == ["is_not_null", "eq"]


def test_relation_where_accepts_the_same_variadic_predicates_as_the_top_level_helper() -> None:
    class Raw(Schema):
        id = string(nullable=False)

    class Published(Schema):
        id = string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            row.where(row.id.is_not_null(), row.id == "accepted")  # type: ignore[attr-defined]
            return Published(id=row.id)

    operations = compile_transform(Publish).steps[0].operations

    assert [operation.kind for operation in operations] == ["filter", "filter"]
    assert [operation.filter.kind for operation in operations if operation.filter is not None] == ["is_not_null", "eq"]


def test_row_project_accepts_the_same_schema_target_as_the_top_level_helper() -> None:
    class Raw(Schema):
        id = string(nullable=False)

    class Published(Schema):
        id = string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return row.project(Published)  # type: ignore[attr-defined]

    assignment = compile_transform(Publish).steps[0].projection[0]

    assert assignment.field.name == "id"
    assert assignment.expression.kind == "field"


def test_membership_predicates_require_values() -> None:
    """Membership predicates need at least one candidate value."""

    class Raw(Schema):
        status = string(nullable=False)

    class Published(Schema):
        known = boolean(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(known=cast(Any, row).status.isin())

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "isin(...) requires at least one value" in raised.value.diagnostic.problem_text()


def test_string_predicates_are_typed_symbolic_expressions() -> None:
    """I can express string matching without a raw SQL expression."""

    class Raw(Schema):
        status = string(nullable=True)

    class Published(Schema):
        has_new = boolean(nullable=True)
        is_new = boolean(nullable=True)
        is_new_case_insensitive = boolean(nullable=True)
        has_release_number = boolean(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

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

    class Raw(Schema):
        count = integer(nullable=False)

    class Published(Schema):
        matched = boolean(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(matched=cast(Any, row).count.contains("1"))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "contains(...) requires a String Structure expression" in raised.value.diagnostic.problem_text()


def test_collection_indexing_is_typed_and_symbolic() -> None:
    """I can read an array item or map value without dropping into a raw hook."""

    class Raw(Schema):
        tags = array(string(), contains_null=False, nullable=False)
        attributes = map(string(), string(), value_contains_null=False, nullable=False)

    class Published(Schema):
        first_tag = string(nullable=True)
        region = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            source = cast(Any, row)
            return Published(first_tag=source.tags[0], region=source.attributes["region"])

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [(expression.kind, expression.type, expression.nullable) for expression in projection.values()] == [
        ("item", types.string(), True),
        ("item", types.string(), True),
    ]
    assert [expression.args[1].data for expression in projection.values()] == [{"value": 0}, {"value": "region"}]


def test_collection_indexing_requires_a_matching_collection_and_key_type() -> None:
    """I get compile diagnostics for invalid collection indexing instead of a Spark runtime error."""

    class Raw(Schema):
        status = string(nullable=False)

    class Published(Schema):
        value = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=cast(Any, row).status[0])

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "Indexing requires an Array or Map Structure expression" in raised.value.diagnostic.problem_text()


def test_scalar_casts_are_typed_symbolic_expressions() -> None:
    """I can cast a value without hiding its target type in a raw hook."""

    class Raw(Schema):
        raw_count = string(nullable=True)
        count = integer(nullable=False)

    class Published(Schema):
        count = integer(nullable=True)
        count_text = string(nullable=False)
        try_count = integer(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            source = cast(Any, row)
            return Published(
                count=source.raw_count.cast(types.integer()),
                count_text=source.count.astype(types.string()),
                try_count=source.raw_count.try_cast(types.integer()),
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

    class Raw(Schema):
        raw_count = string(nullable=True)

    class Published(Schema):
        count = integer(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(count=cast(Any, row).raw_count.cast("int"))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "cast(...) requires a scalar Structure type" in raised.value.diagnostic.problem_text()


def test_string_sql_helpers_are_typed_symbolic_expressions() -> None:
    """I can keep common string shaping and parsing visible to the compiler."""

    class Raw(Schema):
        label = string(nullable=True)

    class Published(Schema):
        prefix = string(nullable=True)
        parts = array(string(), contains_null=False, nullable=True)
        normalized = string(nullable=True)
        extracted = string(nullable=True)
        character_count = integer(nullable=True)
        title = string(nullable=True)
        backward = string(nullable=True)
        normalized_letters = string(nullable=True)
        dash_position = integer(nullable=True)
        distance = integer(nullable=True)
        label = string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                prefix=substring(row.label, start=1, length=3),
                parts=split(row.label, pattern="-"),
                normalized=regexp_replace(row.label, pattern=r"\s+", replacement=" "),
                extracted=regexp_extract(row.label, pattern=r"^([^-]+)", group=1),
                character_count=length(row.label),
                title=initcap(row.label),
                backward=reverse(row.label),
                normalized_letters=translate(row.label, matching="-", replacement="_"),
                dash_position=instr(row.label, substring="-"),
                distance=levenshtein(row.label, "release"),
                label=concat_ws(" / ", row.label, "release"),
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [
        (expression.data, expression.type.name if expression.type else None) for expression in projection.values()
    ] == [
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

    class Raw(Schema):
        count = integer(nullable=False)

    class Published(Schema):
        value = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=substring(row.count, start=1, length=3))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "substring(...) requires a String Structure expression" in raised.value.diagnostic.problem_text()


def test_concat_ws_requires_string_values() -> None:
    """I get a compile diagnostic before invalid concatenation reaches Spark."""

    class Raw(Schema):
        count = integer(nullable=False)

    class Published(Schema):
        value = string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=concat_ws("-", row.count))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "concat_ws(...) requires a String Structure expression" in raised.value.diagnostic.problem_text()


def test_regexp_extract_requires_a_non_negative_group() -> None:
    """I get a compile diagnostic for an invalid capture-group index."""

    class Raw(Schema):
        label = string(nullable=False)

    class Published(Schema):
        value = string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=regexp_extract(row.label, pattern=r"(.*)", group=-1))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "regexp_extract(...) group must be a non-negative integer" in raised.value.diagnostic.problem_text()


def test_temporal_sql_helpers_are_typed_symbolic_expressions() -> None:
    """I can derive dates and time buckets without a raw PySpark hook."""

    class Raw(Schema):
        start_date = date(nullable=False)
        end_date = date(nullable=True)
        recorded_at = timestamp(nullable=True)

    class Published(Schema):
        due_date = date(nullable=False)
        elapsed_days = integer(nullable=True)
        month = timestamp(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                due_date=date_add(row.start_date, days=7),
                elapsed_days=datediff(row.end_date, row.start_date),
                month=date_trunc(row.recorded_at, unit="month"),
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [
        (expression.data, expression.type.name if expression.type else None, expression.nullable)
        for expression in projection.values()
    ] == [
        ({"function": "date_add", "days": 7}, "date", False),
        ({"function": "datediff"}, "integer", True),
        ({"function": "date_trunc", "unit": "month"}, "timestamp", True),
    ]


def test_temporal_sql_helpers_require_date_or_timestamp_inputs() -> None:
    """I get a compile diagnostic before an invalid temporal helper reaches Spark."""

    class Raw(Schema):
        count = integer(nullable=False)

    class Published(Schema):
        due_date = date(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(due_date=date_add(row.count, days=1))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "date_add(...) requires a Date or Timestamp Structure expression" in raised.value.diagnostic.problem_text()


def test_numeric_sql_helpers_are_typed_symbolic_expressions() -> None:
    """I can apply deterministic numeric rounding without a raw PySpark hook."""

    class Raw(Schema):
        amount = decimal(precision=12, scale=2, nullable=True)

    class Published(Schema):
        absolute_amount = decimal(precision=12, scale=2, nullable=True)
        rounded_amount = decimal(precision=12, scale=1, nullable=True)
        ceiling = decimal(precision=11, scale=0, nullable=True)
        floor = decimal(precision=11, scale=0, nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                absolute_amount=abs(row.amount),
                rounded_amount=round(row.amount, scale=1),
                ceiling=ceil(row.amount),
                floor=floor(row.amount),
            )

    projection = {
        assignment.field.name: assignment.expression for assignment in compile_transform(Publish).steps[0].projection
    }

    assert [
        (expression.data, expression.type.name if expression.type else None) for expression in projection.values()
    ] == [
        ({"function": "abs"}, "decimal"),
        ({"function": "round", "scale": 1}, "decimal"),
        ({"function": "ceil"}, "decimal"),
        ({"function": "floor"}, "decimal"),
    ]


def test_numeric_sql_helpers_require_numeric_inputs() -> None:
    """I get a compile diagnostic instead of a Spark type error for an invalid numeric helper."""

    class Raw(Schema):
        label = string(nullable=False)

    class Published(Schema):
        value = decimal(precision=11, scale=0, nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(value=ceil(row.label))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "ceil(...) requires a numeric Structure expression" in raised.value.diagnostic.problem_text()


def test_predicate_sql_helpers_are_typed_symbolic_expressions() -> None:
    """I can use function-style null and NaN checks in compiler-visible predicates."""

    class Raw(Schema):
        label = string(nullable=True)
        score = double(nullable=True)

    class Published(Schema):
        missing_label = boolean(nullable=False)
        present_label = boolean(nullable=False)
        invalid_score = boolean(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                missing_label=isnull(row.label),
                present_label=isnotnull(row.label),
                invalid_score=isnan(row.score),
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

    class Raw(Schema):
        count = integer(nullable=False)

    class Published(Schema):
        invalid = boolean(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(invalid=isnan(row.count))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "isnan(...) requires a Float or Double Structure expression" in raised.value.diagnostic.problem_text()


def test_struct_get_field_is_a_typed_symbolic_expression() -> None:
    """I can read a Struct field by its declared name without a raw Column escape hatch."""

    class Address(Schema):
        city = string(nullable=False, alias='city-name')

    class Raw(Schema):
        address = struct(Address, nullable=True)

    class Published(Schema):
        city = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(city=cast(Any, row).address.get_field("city"))

    expression = compile_transform(Publish).steps[0].projection[0].expression

    assert expression.kind == "get_field"
    assert expression.type == types.string()
    assert expression.nullable
    assert expression.data == {"field": "city-name", "name": "city"}


def test_struct_get_field_rejects_unknown_fields() -> None:
    """I get a compiler diagnostic when a declared Struct does not contain the requested field."""

    class Address(Schema):
        city = string(nullable=False)

    class Raw(Schema):
        address = struct(Address, nullable=False)

    class Published(Schema):
        city = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(city=cast(Any, row).address.get_field("postal_code"))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "get_field(...) cannot find 'postal_code' in Address" in raised.value.diagnostic.problem_text()


def test_lookup_join_requires_boolean_expression() -> None:
    """Join predicates reject non-boolean expressions before target lowering."""

    class Raw(Schema):
        id = string(nullable=False)
        total = integer(nullable=False)

    class Lookup(Schema):
        id = string(nullable=False)

    class Published(Schema):
        id = string(nullable=False)

    @transform
    class BadJoin(Transform):
        rows = input(Raw)
        lookups = input(Lookup)
        published = output(Published)

        def publish(self, row: Raw, lookup: Lookup) -> Published:
            lookup_join(lookup, on=row.total)
            return Published(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadJoin)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "lookup_join(on=...) requires a boolean Structure expression" in raised.value.diagnostic.problem_text()


def test_bare_when_requires_otherwise() -> None:
    """A conditional expression is complete only after otherwise(...)."""

    class Raw(Schema):
        total = integer(nullable=False)

    class Published(Schema):
        size_tier = string(nullable=False)

    @transform
    class BadWhen(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            order = cast(Any, row)
            return Published(size_tier=when(order.total >= 1000, "large"))

    with pytest.raises(TypeError, match=r"when\(\.\.\.\) must end with \.otherwise\(\.\.\.\)"):
        compile_transform(BadWhen)
