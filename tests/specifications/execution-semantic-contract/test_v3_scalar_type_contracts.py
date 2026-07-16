from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast

import pytest

from structure import *
from structure.app.dsl.api import compile_transform
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.expressions import literal
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


class RequiredDecimalSource(Schema):
    raw_amount = field.string(nullable=False)


class RequiredDecimalTarget(Schema):
    amount = field.decimal(12, 2, nullable=False)


class DecimalArithmeticSource(Schema):
    amount = field.decimal(12, 2, nullable=False)


class DecimalArithmeticTarget(Schema):
    amount = field.decimal(12, 2, nullable=False)


class DecimalLiteralTarget(Schema):
    amount = field.decimal(5, 2, nullable=False)


class DecimalWindowSource(Schema):
    tenant = field.string(nullable=False)
    sequence = field.long(nullable=False)
    amount = field.decimal(12, 2, nullable=False)


class DecimalWindowTarget(Schema):
    amount = field.decimal(12, 2, nullable=False)


class RequiredWhenTarget(Schema):
    label = field.string(nullable=False)


class RequiredLookupSource(Schema):
    labels = field.array(field.string(), contains_null=False, nullable=False)


class RequiredLookupTarget(Schema):
    label = field.string(nullable=False)


class RequiredNestedDetails(Schema):
    label = field.string(nullable=True)


class RequiredNestedSource(Schema):
    details = field.struct(RequiredNestedDetails, nullable=False)


class RequiredUdfSource(Schema):
    label = field.string(nullable=False)


class RequiredNestedOutputDetails(Schema):
    label = field.string(nullable=False)


class RequiredNestedOutput(Schema):
    details = field.struct(RequiredNestedOutputDetails, nullable=False)


class RequiredNestedIntegerDetails(Schema):
    amount = field.integer(nullable=False)


class RequiredNestedIntegerOutput(Schema):
    details = field.struct(RequiredNestedIntegerDetails, nullable=False)


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


@transform
class RequiredDecimalParse(Transform):
    source = input(RequiredDecimalSource)
    target = output(RequiredDecimalTarget)

    def normalize(self, row: RequiredDecimalSource) -> RequiredDecimalTarget:
        return RequiredDecimalTarget(amount=to_decimal(row.raw_amount, precision=12, scale=2))


@transform
class NarrowingDecimalArithmetic(Transform):
    source = input(DecimalArithmeticSource)
    target = output(DecimalArithmeticTarget)

    def normalize(self, row: DecimalArithmeticSource) -> DecimalArithmeticTarget:
        return DecimalArithmeticTarget(amount=row.amount + 1)


@transform
class ExplicitDecimalArithmetic(Transform):
    source = input(DecimalArithmeticSource)
    target = output(DecimalArithmeticTarget)

    def normalize(self, row: DecimalArithmeticSource) -> DecimalArithmeticTarget:
        return DecimalArithmeticTarget(amount=(row.amount + 1).cast(types.decimal(12, 2)))


@transform
class DecimalLiteralProjection(Transform):
    source = input(DecimalArithmeticSource)
    target = output(DecimalLiteralTarget)

    def normalize(self, row: DecimalArithmeticSource) -> DecimalLiteralTarget:
        return DecimalLiteralTarget(amount=Decimal("12.34"))


@transform
class IncompatibleComparison(Transform):
    source = input(DecimalArithmeticSource)
    target = output(RequiredPredicateTarget)

    def normalize(self, row: DecimalArithmeticSource) -> RequiredPredicateTarget:
        return RequiredPredicateTarget(accepted=row.amount == "one")


@transform
class IncompatibleComparisonFilter(Transform):
    source = input(DecimalArithmeticSource)
    target = output(DecimalArithmeticTarget)

    def normalize(self, row: DecimalArithmeticSource) -> DecimalArithmeticTarget:
        where(row.amount == "one")
        return DecimalArithmeticTarget(amount=row.amount)


@transform
class DecimalWindowDefault(Transform):
    source = input(DecimalWindowSource)
    target = output(DecimalWindowTarget)

    def normalize(self, row: DecimalWindowSource) -> DecimalWindowTarget:
        return DecimalWindowTarget(
            amount=lag(row.amount, partition_by=row.tenant, order_by=row.sequence, default=Decimal("0.00"))
        )


@transform
class RequiredWhen(Transform):
    source = input(NullablePredicateSource)
    target = output(RequiredWhenTarget)

    def normalize(self, row: NullablePredicateSource) -> RequiredWhenTarget:
        return RequiredWhenTarget(label=when(row.enabled, "enabled").otherwise("disabled"))


@transform
class RequiredLookup(Transform):
    source = input(RequiredLookupSource)
    target = output(RequiredLookupTarget)

    def normalize(self, row: RequiredLookupSource) -> RequiredLookupTarget:
        return RequiredLookupTarget(label=row.labels[1])


@transform
class RequiredNestedLookup(Transform):
    source = input(RequiredNestedSource)
    target = output(RequiredLookupTarget)

    def normalize(self, row: RequiredNestedSource) -> RequiredLookupTarget:
        return RequiredLookupTarget(label=row.details.get_field("label"))


@transform
class RequiredUdf(Transform):
    source = input(RequiredUdfSource)
    target = output(RequiredLookupTarget)

    @special(type="udf", return_type=types.string(), nullable=True)
    def normalize_label(value: Any) -> str:
        return value

    def normalize(self, row: RequiredUdfSource) -> RequiredLookupTarget:
        return RequiredLookupTarget(label=self.normalize_label(row.label))


@transform
class RequiredNestedConstruction(Transform):
    source = input(NullablePredicateSource)
    target = output(RequiredNestedOutput)

    def normalize(self, row: NullablePredicateSource) -> RequiredNestedOutput:
        return RequiredNestedOutput(details=RequiredNestedOutputDetails(label=row.label))


@transform
class IncompatibleNestedConstruction(Transform):
    source = input(NullablePredicateSource)
    target = output(RequiredNestedIntegerOutput)

    def normalize(self, row: NullablePredicateSource) -> RequiredNestedIntegerOutput:
        return RequiredNestedIntegerOutput(details=RequiredNestedIntegerDetails(amount="not an integer"))


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


@pytest.mark.parametrize("value", [array("value"), object()])
def test_to_decimal_rejects_untyped_and_collection_values(value: object) -> None:
    with pytest.raises(TypeError, match=r"to_decimal\(\.\.\.\) requires a String, Boolean, or numeric"):
        to_decimal(value, precision=12, scale=2)


def test_to_decimal_is_nullable_for_required_input() -> None:
    converted = to_decimal(_expression(types.string(), nullable=False), precision=12, scale=2)

    assert converted.nullable is True


def test_to_decimal_cannot_fill_a_required_output_without_a_fallback() -> None:
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(RequiredDecimalParse)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_when_with_required_branches_can_fill_a_required_output_despite_a_nullable_condition() -> None:
    compile_transform(RequiredWhen)


def test_collection_lookup_cannot_fill_a_required_output() -> None:
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(RequiredLookup)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_nullable_struct_field_lookup_cannot_fill_a_required_output() -> None:
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(RequiredNestedLookup)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_nullable_python_udf_cannot_fill_a_required_output() -> None:
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(RequiredUdf)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_nullable_nested_struct_value_cannot_fill_a_required_nested_field() -> None:
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(RequiredNestedConstruction)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_incompatible_nested_struct_value_cannot_fill_a_nested_field() -> None:
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(IncompatibleNestedConstruction)

    assert raised.value.diagnostic.code == "SCHEMA-E0302"


def test_event_time_window_propagates_timestamp_nullability() -> None:
    nullable = window(_expression(types.timestamp(), nullable=True), "1 minute")
    required = window(_expression(types.timestamp(), nullable=False), "1 minute")

    assert nullable.nullable is True
    assert required.nullable is False


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


def test_arithmetic_projects_spark_decimal_precision_and_scale() -> None:
    left = _expression(types.decimal(12, 2), nullable=False)
    right = _expression(types.decimal(10, 4), nullable=True)

    assert _decimal_shape(left + right) == (15, 4)
    assert _decimal_shape(left - right) == (15, 4)
    assert _decimal_shape(left * right) == (23, 6)
    assert _decimal_shape(left + 1) == (13, 2)
    assert _decimal_shape(left + _expression(types.long(), nullable=False)) == (23, 2)
    assert _decimal_shape(left * _expression(types.decimal(38, 20), nullable=False)) == (38, 9)
    assert _decimal_shape(
        _expression(types.decimal(38, 20), nullable=False) * _expression(types.decimal(38, 20), nullable=False)
    ) == (38, 6)


def test_decimal_arithmetic_requires_an_explicit_narrowing_cast() -> None:
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(NarrowingDecimalArithmetic)

    assert raised.value.diagnostic.code == "SCHEMA-E0303"
    compile_transform(ExplicitDecimalArithmetic)


def test_decimal_literals_are_typed_and_imported_by_generated_modules() -> None:
    literal_expression = literal(Decimal("0.0010"))

    assert _decimal_shape(literal_expression) == (4, 4)
    recipe = PySpark.plan.lower()(compile_transform(DecimalLiteralProjection))
    text = PySpark.render.transform()(
        recipe,
        source_transform="tests.DecimalLiteralProjection",
        schema_modules={DecimalArithmeticSource: "tests.schemas", DecimalLiteralTarget: "tests.schemas"},
        runtime_module="tests.runtime",
    )

    assert "from decimal import Decimal" in text
    assert "F.lit(Decimal('12.34'))" in text


def test_lag_accepts_decimal_defaults_and_imports_them_in_generated_modules() -> None:
    recipe = PySpark.plan.lower()(compile_transform(DecimalWindowDefault))
    text = PySpark.render.transform()(
        recipe,
        source_transform="tests.DecimalWindowDefault",
        schema_modules={DecimalWindowSource: "tests.schemas", DecimalWindowTarget: "tests.schemas"},
        runtime_module="tests.runtime",
    )

    assert "from decimal import Decimal" in text
    assert "F.lag(F.col(\"decimal_window_source.amount\"), 1, Decimal('0.00'))" in text


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("1e38")])
def test_decimal_literals_reject_non_finite_and_spark_unsupported_values(value: Decimal) -> None:
    with pytest.raises(TypeError, match="Decimal literals"):
        literal(value)


def _decimal_shape(expression: Expression) -> tuple[int, int]:
    assert isinstance(expression.type, DecimalType)
    return expression.type.precision, expression.type.scale


def test_arithmetic_rejects_non_numeric_operands() -> None:
    with pytest.raises(TypeError, match="Arithmetic requires numeric Structure expressions"):
        _expression(types.integer(), nullable=False) + "one"


def test_comparisons_require_compatible_and_orderable_operands() -> None:
    integer = _expression(types.integer(), nullable=False)

    assert (integer == "one").data == {"comparison_problem": "Comparison requires compatible Structure expression types"}
    assert integer.null_safe_eq("one").data == {
        "comparison_problem": "Comparison requires compatible Structure expression types"
    }
    with pytest.raises(TypeError, match="compatible with its expression type"):
        integer.isin(1, "one")
    assert (integer == object()).data == {"comparison_problem": "Comparison requires compatible Structure expression types"}
    assert (_expression(types.boolean(), nullable=False) > False).data == {
        "comparison_problem": "Ordering comparisons require orderable Structure expression types"
    }
    assert (_expression(types.map(types.string(), types.string()), nullable=False) == None).data == {  # noqa: E711
        "comparison_problem": "Comparison requires compatible Structure expression types"
    }

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(IncompatibleComparison)

    assert raised.value.diagnostic.code == "DSL-E0402"
    with pytest.raises(StructureCompileError) as raised:
        compile_transform(IncompatibleComparisonFilter)

    assert raised.value.diagnostic.code == "DSL-E0402"

    assert (_expression(types.date(), nullable=False) < datetime(2026, 7, 15)).type is not None


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


def test_event_time_between_rejects_a_reversed_interval_range() -> None:
    timestamp = _expression(types.timestamp(), nullable=False)

    with pytest.raises(TypeError, match="lower must not exceed upper"):
        event_time_between(timestamp, timestamp, lower="2 minutes", upper="1 minute")


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
