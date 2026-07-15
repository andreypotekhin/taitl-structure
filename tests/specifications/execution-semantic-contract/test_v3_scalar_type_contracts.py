from datetime import date, datetime
from typing import Any, cast

import pytest

from structure import *
from structure.app.dsl.api import compile_transform
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.target.pyspark.api import PySpark


def _expression(type, *, nullable: bool) -> Expression:
    return Expression(kind="test_value", type=type, nullable=nullable)


class CoalesceSource(Schema):
    amount = field.string(nullable=True)


class CoalesceTarget(Schema):
    amount = field.decimal(12, 2, nullable=False)


class NullablePredicateSource(Schema):
    enabled = field.boolean(nullable=True)
    label = field.string(nullable=True)


class RequiredPredicateTarget(Schema):
    accepted = field.boolean(nullable=False)


@transform
class DecimalFallback(Transform):
    source = input(CoalesceSource)
    target = output(CoalesceTarget)

    def normalize(self, row: CoalesceSource) -> CoalesceTarget:
        return CoalesceTarget(amount=coalesce(0, to_decimal(row.amount, precision=12, scale=2)))


class TemporalSource(Schema):
    tenant = field.string(nullable=False)
    sequence = field.long(nullable=False)
    observed_on = field.date(nullable=True)
    observed_at = field.timestamp(nullable=True)


class TemporalTarget(Schema):
    observed_on = field.date(nullable=False)
    observed_at = field.timestamp(nullable=False)
    previous_observed_on = field.date(nullable=True)
    next_observed_at = field.timestamp(nullable=True)


@transform
class TemporalFallback(Transform):
    source = input(TemporalSource)
    target = output(TemporalTarget)

    def normalize(self, row: TemporalSource) -> TemporalTarget:
        return TemporalTarget(
            observed_on=coalesce(row.observed_on, date(2026, 7, 13)),
            observed_at=coalesce(row.observed_at, datetime(2026, 7, 13, 12, 30)),
            previous_observed_on=lag(
                row.observed_on,
                partition_by=row.tenant,
                order_by=row.sequence,
                default=date(2026, 7, 12),
            ),
            next_observed_at=lead(
                row.observed_at,
                partition_by=row.tenant,
                order_by=row.sequence,
                default=datetime(2026, 7, 13, 12, 31),
            ),
        )


@transform
class NullableNegation(Transform):
    source = input(NullablePredicateSource)
    target = output(RequiredPredicateTarget)

    def normalize(self, row: NullablePredicateSource) -> RequiredPredicateTarget:
        return RequiredPredicateTarget(accepted=~cast(Any, row).enabled)


def test_coalesce_uses_the_common_decimal_type_regardless_of_argument_order() -> None:
    decimal_value = _expression(types.decimal(12, 2), nullable=True)

    expression = coalesce(0, decimal_value)

    assert isinstance(expression.type, DecimalType)
    assert expression.type.precision == 12
    assert expression.type.scale == 2
    assert expression.nullable is False


def test_coalesce_common_type_allows_a_reversed_decimal_fallback_in_a_projection() -> None:
    compile_transform(DecimalFallback)


def test_generated_module_imports_datetime_for_temporal_literals() -> None:
    recipe = PySpark.plan.lower()(compile_transform(TemporalFallback))

    text = PySpark.render.transform()(
        recipe,
        source_transform="tests.TemporalFallback",
        schema_modules={TemporalSource: "tests.schemas", TemporalTarget: "tests.schemas"},
        runtime_module="tests.runtime",
    )

    assert "import datetime" in text
    assert "F.lit(datetime.date(2026, 7, 13))" in text
    assert "F.lit(datetime.datetime(2026, 7, 13, 12, 30))" in text
    assert "F.lag(F.col(\"temporal_source.observed_on\"), 1, datetime.date(2026, 7, 12))" in text
    assert "F.lead(F.col(\"temporal_source.observed_at\"), 1, datetime.datetime(2026, 7, 13, 12, 31))" in text


def test_when_uses_the_common_type_regardless_of_branch_order() -> None:
    expression = when(True, 0).otherwise(_expression(types.decimal(12, 2), nullable=True))

    assert isinstance(expression.type, DecimalType)
    assert expression.type.precision == 12
    assert expression.type.scale == 2
    assert expression.nullable is True


def test_coalesce_remains_nullable_when_all_arguments_can_be_null() -> None:
    expression = coalesce(
        _expression(types.string(), nullable=True),
        _expression(types.string(), nullable=True),
    )

    assert expression.nullable is True


@pytest.mark.parametrize(
    "values",
    [
        (),
        ("text", 1),
        (_expression(types.decimal(38, 38), nullable=True), 2**31),
    ],
)
def test_coalesce_rejects_unknown_or_incompatible_type_combinations(values) -> None:
    with pytest.raises(TypeError):
        coalesce(*values)


def test_when_rejects_incompatible_branch_types() -> None:
    with pytest.raises(TypeError, match=r"when\(\.\.\.\).otherwise\(\.\.\.\) requires compatible types"):
        when(True, "text").otherwise(1)


@pytest.mark.parametrize(
    ("precision", "scale", "message"),
    [
        (39, 0, "precision must be an integer from 1 through 38"),
        (True, 0, "precision must be an integer from 1 through 38"),
        (12, cast(int, 1.5), "scale must be an integer from 0 through precision"),
        (12, 13, "scale must be an integer from 0 through precision"),
    ],
)
def test_decimal_type_rejects_values_outside_spark_domain(precision: int, scale: int, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        types.decimal(precision, scale)


def test_to_decimal_rejects_precision_larger_than_spark_supports() -> None:
    with pytest.raises(ValueError, match="precision must be an integer from 1 through 38"):
        to_decimal("1", precision=39, scale=0)


@pytest.mark.parametrize(
    ("scale", "precision", "result_scale"),
    [(-4, 11, 0), (0, 11, 0), (1, 12, 1), (4, 13, 2)],
)
def test_round_projects_decimal_precision_and_scale_to_spark_contract(
    scale: int, precision: int, result_scale: int
) -> None:
    expression = round(_expression(types.decimal(12, 2), nullable=False), scale=scale)

    assert isinstance(expression.type, DecimalType)
    assert (expression.type.precision, expression.type.scale) == (precision, result_scale)
    assert expression.nullable is False


def test_round_preserves_non_decimal_numeric_type() -> None:
    expression = round(_expression(types.integer(), nullable=True), scale=-1)

    assert expression.type is not None and expression.type.name == "integer"
    assert expression.nullable is True


@pytest.mark.parametrize("helper", [bool_and, bool_or])
def test_boolean_aggregates_are_required_for_required_unfiltered_values(helper) -> None:
    required = helper(_expression(types.boolean(), nullable=False))
    nullable = helper(_expression(types.boolean(), nullable=True))
    filtered = helper(_expression(types.boolean(), nullable=False), where=True)

    assert required.nullable is False
    assert nullable.nullable is True
    assert filtered.nullable is True


def test_arithmetic_widens_numeric_types_and_propagates_nullability() -> None:
    integer = _expression(types.integer(), nullable=False)
    nullable_long = _expression(types.long(), nullable=True)

    widened = integer + 0.5
    nullable = integer + nullable_long

    assert widened.type is not None and widened.type.name == "double"
    assert widened.nullable is False
    assert nullable.type is not None and nullable.type.name == "long"
    assert nullable.nullable is True


def test_arithmetic_rejects_non_numeric_operands() -> None:
    with pytest.raises(TypeError, match="Arithmetic requires numeric Structure expressions"):
        _expression(types.integer(), nullable=False) + "one"


@pytest.mark.parametrize("helper", [lower, trim, upper])
def test_string_normalizers_require_and_return_string_expressions(helper) -> None:
    normalized = helper(_expression(types.string(), nullable=False))

    assert normalized.type is not None and normalized.type.name == "string"
    assert normalized.nullable is False
    with pytest.raises(TypeError, match=r"requires a String Structure expression"):
        helper(1)


@pytest.mark.parametrize("interval", ["", "one minute", "-1 second", "1 second; SELECT 1"])
def test_event_time_between_rejects_invalid_interval_text(interval: str) -> None:
    timestamp = _expression(types.timestamp(), nullable=False)

    with pytest.raises(TypeError, match="requires a non-negative fixed Spark interval"):
        event_time_between(timestamp, timestamp, upper=interval)
    with pytest.raises(TypeError, match="requires a non-negative fixed Spark interval"):
        event_time_between(timestamp, timestamp, lower=interval, upper="1 second")


@pytest.mark.parametrize(
    "unit", ["year", "YYYY", "quarter", "mon", "dd", "hour", "microsecond"]
)
def test_date_trunc_accepts_only_spark_truncation_units(unit: str) -> None:
    timestamp = _expression(types.timestamp(), nullable=False)

    expression = date_trunc(timestamp, unit=unit)

    assert expression.data == {"function": "date_trunc", "unit": unit.lower()}
    assert expression.nullable is False


@pytest.mark.parametrize("unit", ["", "season", "month; SELECT 1"])
def test_date_trunc_rejects_invalid_units(unit: str) -> None:
    with pytest.raises(TypeError, match="date_trunc\\(\\.\\.\\.\\) unit must be one of"):
        date_trunc(_expression(types.timestamp(), nullable=False), unit=unit)


def test_predicates_propagate_nullable_sql_three_valued_logic() -> None:
    nullable_boolean = _expression(types.boolean(), nullable=True)
    nullable_string = _expression(types.string(), nullable=True)
    non_null_string = _expression(types.string(), nullable=False)
    nullable_timestamp = _expression(types.timestamp(), nullable=True)
    null_safe = nullable_string.null_safe_eq(None)

    assert (nullable_string == "active").nullable is True
    assert (nullable_string != "active").nullable is True
    assert null_safe.type is not None
    assert null_safe.type.name == "boolean"
    assert null_safe.nullable is False
    assert (nullable_boolean & True).nullable is True
    assert (nullable_boolean | False).nullable is True
    assert (~nullable_boolean).nullable is True
    assert non_null_string.isin("active", "held").nullable is False
    assert non_null_string.isin("active", None).nullable is True
    assert event_time_between(nullable_timestamp, datetime(2026, 7, 13), upper="1 hour").nullable is True


@pytest.mark.parametrize(
    "expression",
    [
        lambda: _expression(types.string(), nullable=False) & True,
        lambda: _expression(types.boolean(), nullable=False) | 1,
        lambda: ~_expression(types.string(), nullable=False),
    ],
)
def test_logical_operators_require_boolean_operands(expression) -> None:
    with pytest.raises(TypeError, match="Boolean Structure expression"):
        expression()


def test_nullable_negation_cannot_fill_a_non_nullable_output() -> None:
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(NullableNegation)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"
