from datetime import date, datetime

import pytest

from structure import *
from structure.app.dsl.api import compile_transform
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.target.pyspark.api import PySpark


def _expression(type, *, nullable: bool) -> Expression:
    return Expression(kind="test_value", type=type, nullable=nullable)


class CoalesceSource(Schema):
    amount = field(String(), nullable=True)


class CoalesceTarget(Schema):
    amount = field(Decimal(12, 2), nullable=False)


@transform
class DecimalFallback(Transform):
    source = input(CoalesceSource)
    target = output(CoalesceTarget)

    def normalize(self, row: CoalesceSource) -> CoalesceTarget:
        return CoalesceTarget(amount=coalesce(0, to_decimal(row.amount, precision=12, scale=2)))


class TemporalSource(Schema):
    tenant = field(String(), nullable=False)
    sequence = field(Long(), nullable=False)
    observed_on = field(Date(), nullable=True)
    observed_at = field(Timestamp(), nullable=True)


class TemporalTarget(Schema):
    observed_on = field(Date(), nullable=False)
    observed_at = field(Timestamp(), nullable=False)
    previous_observed_on = field(Date(), nullable=True)
    next_observed_at = field(Timestamp(), nullable=True)


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


def test_coalesce_uses_the_common_decimal_type_regardless_of_argument_order() -> None:
    decimal_value = _expression(Decimal(12, 2), nullable=True)

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
    expression = when(True, 0).otherwise(_expression(Decimal(12, 2), nullable=True))

    assert isinstance(expression.type, DecimalType)
    assert expression.type.precision == 12
    assert expression.type.scale == 2
    assert expression.nullable is True


def test_coalesce_remains_nullable_when_all_arguments_can_be_null() -> None:
    expression = coalesce(
        _expression(String(), nullable=True),
        _expression(String(), nullable=True),
    )

    assert expression.nullable is True


@pytest.mark.parametrize(
    "values",
    [
        (),
        ("text", 1),
        (_expression(Decimal(38, 38), nullable=True), 2**31),
    ],
)
def test_coalesce_rejects_unknown_or_incompatible_type_combinations(values) -> None:
    with pytest.raises(TypeError):
        coalesce(*values)


def test_when_rejects_incompatible_branch_types() -> None:
    with pytest.raises(TypeError, match=r"when\(\.\.\.\).otherwise\(\.\.\.\) requires compatible types"):
        when(True, "text").otherwise(1)
