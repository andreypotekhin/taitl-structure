import datetime
from builtins import float as scalar_float
from decimal import Decimal
from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.types import BinaryType, DecimalType, StringType, StructType


def _compile(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False)


def _expression(type, *, nullable: bool) -> Expression:
    return Expression(kind="test_value", type=type, nullable=nullable)


class CoalesceSource(Schema):
    amount = string(nullable=True)


class CoalesceTarget(Schema):
    amount = decimal(12, 2, nullable=False)


class NullablePredicateSource(Schema):
    enabled = boolean(nullable=True)
    label = string(nullable=True)


class RequiredPredicateTarget(Schema):
    accepted = boolean(nullable=False)


class RequiredDecimalSource(Schema):
    raw_amount = string(nullable=False)


class RequiredDecimalTarget(Schema):
    amount = decimal(12, 2, nullable=False)


class DecimalArithmeticSource(Schema):
    amount = decimal(12, 2, nullable=False)


class DecimalArithmeticTarget(Schema):
    amount = decimal(12, 2, nullable=False)


class DecimalLiteralTarget(Schema):
    amount = decimal(5, 2, nullable=False)


class DecimalWindowSource(Schema):
    tenant = string(nullable=False)
    sequence = long(nullable=False)
    amount = decimal(12, 2, nullable=False)


class DecimalWindowTarget(Schema):
    amount = decimal(12, 2, nullable=False)


class RequiredWhenTarget(Schema):
    label = string(nullable=False)


class RequiredLookupSource(Schema):
    labels = array(string(), contains_null=False, nullable=False)


class RequiredLookupTarget(Schema):
    label = string(nullable=False)


class RequiredNestedDetails(Schema):
    label = string(nullable=True)


class RequiredNestedSource(Schema):
    details = struct(RequiredNestedDetails, nullable=False)


class ParsedPayload(Schema):
    code = string(nullable=True)
    amount = integer(nullable=True)


def test_v7_binary_literals_and_encoding_helpers_have_precise_types() -> None:
    payload = literal(b"paid")
    encoded = base64(payload)
    decoded = unbase64(encoded)
    text_bytes = encode("paid", charset="UTF-8")
    text = decode(payload, charset="UTF-8")

    assert isinstance(payload.type, BinaryType)
    assert payload.data is not None
    assert payload.data["value"] == b"paid"
    assert isinstance(encoded.type, StringType)
    assert encoded.nullable is False
    assert isinstance(decoded.type, BinaryType)
    assert decoded.nullable is True
    assert isinstance(text_bytes.type, BinaryType)
    assert text_bytes.nullable is False
    assert isinstance(text.type, StringType)
    assert text.nullable is False


def test_v7_binary_encoding_helpers_reject_wrong_types_and_invalid_charsets() -> None:
    with pytest.raises(TypeError, match="base64\\(\\.\\.\\.\\) requires a Binary Structure expression"):
        base64("paid")
    with pytest.raises(TypeError, match="unbase64\\(\\.\\.\\.\\) requires a String Structure expression"):
        unbase64(b"cGFpZA==")
    with pytest.raises(TypeError, match="encode\\(\\.\\.\\.\\) charset must be a non-empty string literal"):
        encode("paid", charset="")
    with pytest.raises(TypeError, match="decode\\(\\.\\.\\.\\) requires a Binary Structure expression"):
        decode("paid")


def test_v7_schema_carrying_parsing_helpers_have_precise_types_and_options() -> None:
    parsed_json = from_json('{"code":"paid","amount":2}', as_=ParsedPayload)
    parsed_csv = from_csv("paid|2", as_=ParsedPayload, options=CsvOptions(delimiter="|", null_value=""))
    rendered_json = to_json(parsed_json, options=JsonOptions(date_format="yyyy-MM-dd"))
    rendered_csv = to_csv(parsed_csv, options=CsvOptions(delimiter="|"))

    assert isinstance(parsed_json.type, StructType)
    assert parsed_json.type.schema is ParsedPayload
    assert parsed_json.nullable is True
    assert isinstance(parsed_csv.type, StructType)
    assert parsed_csv.type.schema is ParsedPayload
    assert parsed_csv.data is not None
    assert parsed_csv.data["options"] == {"sep": "|", "nullValue": "", "mode": "PERMISSIVE"}
    assert isinstance(rendered_json.type, StringType)
    assert rendered_json.nullable is True
    assert isinstance(rendered_csv.type, StringType)
    assert rendered_csv.nullable is True


def test_v7_schema_carrying_parsing_helpers_reject_untyped_options_and_schemas() -> None:
    class RequiredPayload(Schema):
        code = string(nullable=False)

    with pytest.raises(TypeError, match="from_json\\(\\.\\.\\.\\) as_= must be a Schema class"):
        from_json("{}", as_=object)
    with pytest.raises(TypeError, match="from_json\\(\\.\\.\\.\\) as_= schema field RequiredPayload.code must be nullable"):
        from_json("{}", as_=RequiredPayload)
    with pytest.raises(TypeError, match="JSON conversion options must be a JsonOptions value"):
        from_json("{}", as_=ParsedPayload, options={})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="CSV conversion options must be a CsvOptions value"):
        from_csv("paid,2", as_=ParsedPayload, options={})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match='CsvOptions.mode must be "PERMISSIVE"'):
        from_csv("paid,2", as_=ParsedPayload, options=CsvOptions(mode="FAILFAST"))
    with pytest.raises(TypeError, match="CsvOptions.delimiter must be a non-empty string or None"):
        from_csv("paid,2", as_=ParsedPayload, options=CsvOptions(delimiter=""))
    with pytest.raises(TypeError, match="to_json\\(\\.\\.\\.\\) requires a Struct Structure expression"):
        to_json("paid")


class RequiredUdfSource(Schema):
    label = string(nullable=False)


class RequiredNestedOutputDetails(Schema):
    label = string(nullable=False)


class RequiredNestedOutput(Schema):
    details = struct(RequiredNestedOutputDetails, nullable=False)


class RequiredNestedIntegerDetails(Schema):
    amount = integer(nullable=False)


class RequiredNestedIntegerOutput(Schema):
    details = struct(RequiredNestedIntegerDetails, nullable=False)


@transform
class DecimalFallback(Transform):
    source = input(CoalesceSource)
    target = output(CoalesceTarget)

    def normalize(self, row: CoalesceSource) -> CoalesceTarget:
        return CoalesceTarget(amount=coalesce(0, to_decimal(row.amount, precision=12, scale=2)))


class TemporalSource(Schema):
    tenant = string(nullable=False)
    sequence = long(nullable=False)
    observed_on = date(nullable=True)
    observed_at = timestamp(nullable=True)


class TemporalTarget(Schema):
    observed_on = date(nullable=False)
    observed_at = timestamp(nullable=False)
    previous_observed_on = date(nullable=True)
    next_observed_at = timestamp(nullable=True)


@transform
class TemporalFallback(Transform):
    source = input(TemporalSource)
    target = output(TemporalTarget)

    def normalize(self, row: TemporalSource) -> TemporalTarget:
        return TemporalTarget(
            observed_on=coalesce(row.observed_on, datetime.date(2026, 7, 13)),
            observed_at=coalesce(row.observed_at, datetime.datetime(2026, 7, 13, 12, 30)),
            previous_observed_on=lag(
                row.observed_on,
                partition_by=row.tenant,
                order_by=row.sequence,
                default=datetime.date(2026, 7, 12),
            ),
            next_observed_at=lead(
                row.observed_at,
                partition_by=row.tenant,
                order_by=row.sequence,
                default=datetime.datetime(2026, 7, 13, 12, 31),
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
    _compile(DecimalFallback)


def test_generated_module_imports_datetime_for_temporal_literals() -> None:
    recipe = cast(PySparkExecutionPlan, _compile(TemporalFallback).lowered)

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


def test_nullif_preserves_the_left_type_and_is_always_nullable() -> None:
    required = _expression(types.integer(), nullable=False)
    nullable = _expression(types.string(), nullable=True)

    integer = nullif(required, 0)
    label = nullif(nullable, "unknown")

    assert integer.type is not None and integer.type.name == "integer"
    assert integer.nullable is True
    assert label.type is not None and label.type.name == "string"
    assert label.nullable is True


@pytest.mark.parametrize(
    "values",
    [
        (None, "unknown"),
        (_expression(types.integer(), nullable=False), "unknown"),
        (_expression(types.map(types.string(), types.string()), nullable=False), None),
    ],
)
def test_nullif_requires_a_typed_left_expression_and_comparable_values(values: tuple[object, object]) -> None:
    with pytest.raises(TypeError, match=r"nullif\(\.\.\.\) requires"):
        nullif(*values)


def test_nanvl_returns_double_and_propagates_nullability() -> None:
    required_float = _expression(types.float(), nullable=False)
    nullable_double = _expression(types.double(), nullable=True)

    fallback = nanvl(required_float, nullable_double)

    assert fallback.type is not None and fallback.type.name == "double"
    assert fallback.nullable is True


@pytest.mark.parametrize("values", [(1.0, 1), (_expression(types.integer(), nullable=False), 1.0)])
def test_nanvl_requires_floating_point_values(values: tuple[object, object]) -> None:
    with pytest.raises(TypeError, match=r"nanvl\(\.\.\.\) requires Float or Double Structure expressions"):
        nanvl(*values)


def test_deterministic_numeric_helpers_return_typed_null_propagating_expressions() -> None:
    nullable_decimal = _expression(types.decimal(12, 2), nullable=True)
    required_integer = _expression(types.integer(), nullable=False)

    rounded = bround(nullable_decimal, scale=1)
    assert isinstance(rounded.type, DecimalType)
    assert rounded.type.precision == 12 and rounded.type.scale == 1
    assert rounded.nullable is True
    for expression in (
        acos(nullable_decimal),
        acosh(nullable_decimal),
        asin(nullable_decimal),
        asinh(nullable_decimal),
        atan(nullable_decimal),
        atanh(nullable_decimal),
        cbrt(nullable_decimal),
        cos(nullable_decimal),
        cosh(nullable_decimal),
        cot(nullable_decimal),
        csc(nullable_decimal),
        degrees(nullable_decimal),
        expm1(nullable_decimal),
        hypot(nullable_decimal, required_integer),
        ln(nullable_decimal),
        log10(nullable_decimal),
        log1p(nullable_decimal),
        log2(nullable_decimal),
        sqrt(nullable_decimal),
        pow(nullable_decimal, required_integer),
        log(nullable_decimal),
        log(nullable_decimal, base=10),
        radians(nullable_decimal),
        rint(nullable_decimal),
        sec(nullable_decimal),
        sign(nullable_decimal),
        sin(nullable_decimal),
        sinh(nullable_decimal),
        exp(nullable_decimal),
        signum(nullable_decimal),
        tan(nullable_decimal),
        tanh(nullable_decimal),
    ):
        assert expression.type is not None and expression.type.name == "double"
        assert expression.nullable is True
    assert atan2(nullable_decimal, required_integer).nullable is True


def test_remaining_admitted_numeric_helpers_have_typed_contracts() -> None:
    nullable_decimal = _expression(types.decimal(12, 2), nullable=True)
    required_integer = _expression(types.integer(), nullable=False)
    required_long = _expression(types.long(), nullable=False)

    for expression in (e(), pi()):
        assert expression.type is not None and expression.type.name == "double"
        assert expression.nullable is False

    nullable_integer = _expression(types.integer(), nullable=True)
    factorial_value = factorial(nullable_integer)
    assert factorial_value.type is not None and factorial_value.type.name == "long"
    assert factorial_value.nullable is True

    for expression in (greatest(nullable_decimal, required_integer), least(nullable_decimal, required_integer)):
        assert isinstance(expression.type, DecimalType)
        assert expression.type.precision == 12 and expression.type.scale == 2
        assert expression.nullable is False
    assert greatest(nullable_decimal, nullable_integer).nullable is True
    assert least(nullable_decimal, nullable_integer).nullable is True

    positive_modulo = pmod(required_integer, required_long)
    assert positive_modulo.type is not None and positive_modulo.type.name == "long"
    assert positive_modulo.nullable is False

    for expression in (bin(nullable_integer), hex(nullable_integer)):
        assert expression.type is not None and expression.type.name == "string"
        assert expression.nullable is True
    decoded = unhex(_expression(types.string(), nullable=True))
    assert isinstance(decoded.type, BinaryType)
    assert decoded.nullable is True

    with pytest.raises(TypeError, match=r"factorial\(\.\.\.\) requires an integer or long"):
        factorial(1.5)
    with pytest.raises(TypeError, match=r"greatest\(\.\.\.\) requires at least two values"):
        greatest(1)
    with pytest.raises(TypeError, match=r"pmod\(\.\.\.\) requires a numeric Structure expression"):
        pmod("not numeric", 1)
    with pytest.raises(TypeError, match=r"bin\(\.\.\.\) requires an integer or long"):
        bin(1.5)
    with pytest.raises(TypeError, match=r"hex\(\.\.\.\) requires a Binary, Integer, or Long"):
        hex("not numeric or binary")
    with pytest.raises(TypeError, match=r"unhex\(\.\.\.\) requires a String Structure expression"):
        unhex(1)


def test_atan2_requires_two_numeric_operands() -> None:
    with pytest.raises(TypeError, match=r"atan2\(\.\.\.\) requires a numeric Structure expression"):
        atan2("not numeric", 1)


def test_rand_requires_explicit_reproducibility_policy_and_returns_non_null_double() -> None:
    seeded = rand(seed=17)
    unseeded = rand(reproducible=False)

    assert seeded.type is not None and seeded.type.name == "double"
    assert seeded.nullable is False
    assert dict(seeded.data or {}) == {
        "function": "rand",
        "seed": 17,
        "reproducible": True,
        "nondeterministic": True,
    }
    assert dict(unseeded.data or {})["seed"] is None
    assert dict(unseeded.data or {})["reproducible"] is False


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda: rand(), "seed is required"),
        (lambda: rand(seed=True), "seed must be an integer"),
        (lambda: rand(seed=1, reproducible=cast(bool, "yes")), "reproducible must be a Boolean"),
    ],
)
def test_rand_rejects_implicit_or_invalid_seed_policy(call, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        call()


@pytest.mark.parametrize(
    "function",
    [
        acos, acosh, asin, asinh, atan, atanh, cbrt, cos, cosh, cot, csc, degrees, exp, expm1, ln, log, log1p,
        log2, log10, radians, rint, sec, sign, signum, sin, sinh, sqrt, tan, tanh,
    ],
)
def test_unary_deterministic_numeric_helpers_require_numeric_values(function) -> None:
    with pytest.raises(TypeError, match=r"requires a numeric Structure expression"):
        function("not numeric")


@pytest.mark.parametrize("base", [True, 0, 1, -2, scalar_float("inf"), scalar_float("nan"), "ten"])
def test_log_requires_a_valid_literal_base(base: object) -> None:
    with pytest.raises(TypeError, match=r"log\(\.\.\.\) base must be a positive numeric literal other than 1"):
        log(1.0, base=base)  # type: ignore[arg-type]


def test_hash_helpers_have_typed_results_and_explicit_null_contracts() -> None:
    nullable_label = _expression(types.string(), nullable=True)
    required_id = _expression(types.long(), nullable=False)

    hash_code = hash(nullable_label, required_id)
    long_hash = xxhash64(nullable_label)
    assert hash_code.type is not None and hash_code.type.name == "integer"
    assert hash(nullable_label).nullable is True
    assert long_hash.type is not None and long_hash.type.name == "long"
    assert long_hash.nullable is True
    for expression in (md5(nullable_label), sha1(nullable_label), sha2(nullable_label, bits=512)):
        assert expression.type is not None and expression.type.name == "string"
        assert expression.nullable is True


def test_null_control_helpers_preserve_typed_branch_contracts() -> None:
    nullable_label = _expression(types.string(), nullable=True)
    required_label = _expression(types.string(), nullable=False)
    nullable_amount = _expression(types.decimal(12, 2), nullable=True)

    assert nvl(nullable_label, "unknown").nullable is False
    fallback = ifnull(nullable_label, "unknown")
    assert fallback.type is not None and fallback.type.name == "string"
    assert nvl2(nullable_label, required_label, "missing").nullable is False
    assert zeroifnull(nullable_amount).type is nullable_amount.type
    assert zeroifnull(nullable_amount).nullable is False


def test_zeroifnull_requires_a_numeric_expression() -> None:
    with pytest.raises(TypeError, match=r"zeroifnull\(\.\.\.\) requires a numeric Structure expression"):
        zeroifnull("none")


@pytest.mark.parametrize("function", [hash, xxhash64])
def test_hash_helpers_require_scalar_values(function) -> None:
    with pytest.raises(TypeError, match=r"requires scalar Structure expressions"):
        function(_expression(types.map(types.string(), types.string()), nullable=False))


@pytest.mark.parametrize("bits", [0, 128, 225, 1024])
def test_sha2_requires_a_supported_digest_length(bits: int) -> None:
    with pytest.raises(TypeError, match=r"sha2\(\.\.\.\) bits must be one of"):
        sha2("value", bits=bits)


def test_temporal_helpers_preserve_typed_calendar_contracts() -> None:
    required_date = _expression(types.date(), nullable=False)
    nullable_timestamp = _expression(types.timestamp(), nullable=True)
    required_text = _expression(types.string(), nullable=False)

    previous = date_sub(required_date, days=1)
    month_start = trunc(required_date, unit="month")
    assert previous.type is not None and previous.type.name == "date"
    assert month_start.type is not None and month_start.type.name == "date"
    for expression in (year(required_date), month(required_date), dayofmonth(required_date)):
        assert expression.type is not None and expression.type.name == "integer"
        assert expression.nullable is False
    for expression in (hour(nullable_timestamp), minute(nullable_timestamp), second(nullable_timestamp)):
        assert expression.type is not None and expression.type.name == "integer"
        assert expression.nullable is True
    assert to_date(required_text, format="yyyy-MM-dd").nullable is True
    assert to_timestamp(required_text, format="yyyy-MM-dd HH:mm:ss").nullable is True


def test_calendar_and_padding_helpers_preserve_typed_contracts() -> None:
    required_date = _expression(types.date(), nullable=False)
    nullable_timestamp = _expression(types.timestamp(), nullable=True)
    nullable_text = _expression(types.string(), nullable=True)

    assert add_months(required_date, months=2).type is not None
    assert add_months(nullable_timestamp, months=1).nullable is True
    assert add_months(required_date, months=_expression(types.integer(), nullable=True)).nullable is True
    assert next_day(required_date, day_of_week="Mon").type is not None
    assert next_day(nullable_timestamp, day_of_week="Monday").nullable is True
    assert lpad(nullable_text, length=8, pad="0").nullable is True
    assert rpad(nullable_text, length=8, pad="0").nullable is True


@pytest.mark.parametrize("function", [lpad, rpad])
def test_padding_helpers_require_valid_literal_arguments(function) -> None:
    with pytest.raises(TypeError, match=r"requires a String Structure expression"):
        function(1, length=2, pad="0")
    with pytest.raises(TypeError, match=r"length must be a non-negative integer literal"):
        function("value", length=-1, pad="0")
    with pytest.raises(TypeError, match=r"pad must be a non-empty string literal"):
        function("value", length=2, pad="")


def test_string_slicing_position_and_byte_helpers_preserve_types() -> None:
    nullable_text = _expression(types.string(), nullable=True)
    required_binary = _expression(types.binary(), nullable=False)

    for expression in (
        ascii(nullable_text),
        char_length(nullable_text),
        locate(nullable_text, substring="a"),
        octet_length(nullable_text),
    ):
        assert expression.type is not None and expression.type.name == "integer"
        assert expression.nullable is True
    assert octet_length(required_binary).nullable is False
    for expression in (
        left(nullable_text, length=2),
        repeat(nullable_text, count=2),
        replace(nullable_text, search="a", replacement="b"),
        right(nullable_text, length=2),
        substring_index(nullable_text, delimiter="/", count=2),
    ):
        assert expression.type is not None and expression.type.name == "string"
        assert expression.nullable is True


@pytest.mark.parametrize(
    ("function", "keyword", "value"),
    [(left, "length", -1), (right, "length", -1), (repeat, "count", -1)],
)
def test_string_count_helpers_require_non_negative_literals(function, keyword: str, value: int) -> None:
    with pytest.raises(TypeError, match=r"must be a non-negative integer literal"):
        function("value", **{keyword: value})


def test_locate_and_substring_index_require_valid_literal_arguments() -> None:
    with pytest.raises(TypeError, match=r"position must be a positive integer literal"):
        locate("value", substring="a", position=0)
    with pytest.raises(TypeError, match=r"count must be an integer literal"):
        substring_index("value", delimiter="/", count=cast(int, "two"))


@pytest.mark.parametrize("day", ["", "weekday", "Monday; SELECT 1", 1])
def test_next_day_requires_a_weekday_literal(day: object) -> None:
    with pytest.raises(TypeError, match=r"day_of_week must name a weekday"):
        next_day(_expression(types.date(), nullable=False), day_of_week=day)  # type: ignore[arg-type]


@pytest.mark.parametrize("function", [hour, minute, second])
def test_time_extraction_requires_timestamp_values(function) -> None:
    with pytest.raises(TypeError, match=r"requires a Timestamp Structure expression"):
        function(_expression(types.date(), nullable=False))


@pytest.mark.parametrize("unit", ["day", "season", "month; SELECT 1"])
def test_trunc_requires_supported_date_units(unit: str) -> None:
    with pytest.raises(TypeError, match=r"trunc\(\.\.\.\) unit must be one of"):
        trunc(_expression(types.date(), nullable=False), unit=unit)


@pytest.mark.parametrize("format", ["", 1])
def test_temporal_conversions_require_non_empty_format_literals(format: object) -> None:
    with pytest.raises(TypeError, match=r"format must be a non-empty string literal"):
        to_date("2026-07-15", format=format)  # type: ignore[arg-type]


@pytest.mark.parametrize("function", [ltrim, rtrim])
def test_one_sided_trim_preserves_string_type_and_nullability(function) -> None:
    expression = function(_expression(types.string(), nullable=True))

    assert expression.type is not None and expression.type.name == "string"
    assert expression.nullable is True


@pytest.mark.parametrize("function", [ltrim, rtrim])
def test_one_sided_trim_requires_a_string_expression(function) -> None:
    with pytest.raises(TypeError, match=r"requires a String Structure expression"):
        function(1)


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
        _compile(RequiredDecimalParse)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_when_with_required_branches_can_fill_a_required_output_despite_a_nullable_condition() -> None:
    _compile(RequiredWhen)


def test_collection_lookup_cannot_fill_a_required_output() -> None:
    with pytest.raises(StructureCompileError) as raised:
        _compile(RequiredLookup)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_nullable_struct_field_lookup_cannot_fill_a_required_output() -> None:
    with pytest.raises(StructureCompileError) as raised:
        _compile(RequiredNestedLookup)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_nullable_python_udf_cannot_fill_a_required_output() -> None:
    with pytest.raises(StructureCompileError) as raised:
        _compile(RequiredUdf)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_nullable_nested_struct_value_cannot_fill_a_required_nested_field() -> None:
    with pytest.raises(StructureCompileError) as raised:
        _compile(RequiredNestedConstruction)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"


def test_incompatible_nested_struct_value_cannot_fill_a_nested_field() -> None:
    with pytest.raises(StructureCompileError) as raised:
        _compile(IncompatibleNestedConstruction)

    assert raised.value.diagnostic.code == "SCHEMA-E0302"


def test_event_time_window_propagates_timestamp_nullability() -> None:
    nullable = window(_expression(types.timestamp(), nullable=True), "1 minute")
    required = window(_expression(types.timestamp(), nullable=False), "1 minute")

    assert nullable.nullable is True
    assert required.nullable is False


def test_session_window_propagates_timestamp_nullability_and_requires_a_fixed_gap() -> None:
    nullable = session_window(_expression(types.timestamp(), nullable=True), "10 minutes")
    required = session_window(_expression(types.timestamp(), nullable=False), "10 minutes")

    assert nullable.type == StructType(TimeWindow)
    assert nullable.nullable is True
    assert required.nullable is False
    with pytest.raises(TypeError, match="positive fixed Spark interval"):
        session_window(_expression(types.timestamp(), nullable=False), "0 minutes")


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


def test_division_modulo_and_negation_have_typed_null_propagating_results() -> None:
    integer = _expression(types.integer(), nullable=False)
    nullable_decimal = _expression(types.decimal(12, 2), nullable=True)

    quotient = integer / 2
    remainder = integer % 2
    decimal_quotient = nullable_decimal / 2
    negated = -nullable_decimal

    assert quotient.type is not None and quotient.type.name == "double"
    assert remainder.type is not None and remainder.type.name == "integer"
    assert isinstance(decimal_quotient.type, DecimalType)
    assert decimal_quotient.nullable is True
    assert negated.type is nullable_decimal.type
    assert negated.nullable is True


def test_declared_struct_mutation_requires_an_exact_result_schema() -> None:
    class Source(Schema):
        label = string(nullable=True)
        rank = integer(nullable=False)

    class Replaced(Schema):
        label = string(nullable=False)
        rank = integer(nullable=False)

    class Dropped(Schema):
        label = string(nullable=True)

    value = _expression(types.struct(Source), nullable=True)

    replaced = value.with_field("label", "known", schema=Replaced)
    dropped = value.drop_fields("rank", schema=Dropped)

    assert replaced.type is not None and replaced.type.name == "struct"
    assert replaced.nullable is True
    assert dropped.data == {"fields": ("rank",)}


def test_bitwise_operations_preserve_integral_types_and_nullability() -> None:
    integer = _expression(types.integer(), nullable=False)
    nullable_long = _expression(types.long(), nullable=True)
    intersected = integer.bitwise_and(3)
    combined = integer.bitwise_or(nullable_long)
    changed = integer.bitwise_xor(nullable_long)
    inverted = nullable_long.bitwise_not()

    assert intersected.kind == "bitwise_and"
    assert intersected.type is not None and intersected.type.name == "integer"
    assert combined.type is not None and combined.type.name == "long"
    assert changed.nullable is True
    assert inverted.type is not None and inverted.type.name == "long"
    assert inverted.nullable is True


@pytest.mark.parametrize(
    "expression",
    [
        lambda: _expression(types.integer(), nullable=False).bitwise_and(0.5),
        lambda: _expression(types.integer(), nullable=False).bitwise_or(True),
        lambda: _expression(types.decimal(8, 0), nullable=False).bitwise_xor(1),
        lambda: _expression(types.string(), nullable=False).bitwise_not(),
    ],
)
def test_bitwise_operations_require_integral_operands(expression) -> None:
    with pytest.raises(TypeError, match="Bitwise operations require integral Structure expressions"):
        expression()


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
        _compile(NarrowingDecimalArithmetic)

    assert raised.value.diagnostic.code == "SCHEMA-E0303"
    _compile(ExplicitDecimalArithmetic)


def test_decimal_literals_are_typed_and_imported_by_generated_modules() -> None:
    literal_expression = literal(Decimal("0.0010"))

    assert _decimal_shape(literal_expression) == (4, 4)
    recipe = cast(PySparkExecutionPlan, _compile(DecimalLiteralProjection).lowered)
    text = PySpark.render.transform()(
        recipe,
        source_transform="tests.DecimalLiteralProjection",
        schema_modules={DecimalArithmeticSource: "tests.schemas", DecimalLiteralTarget: "tests.schemas"},
        runtime_module="tests.runtime",
    )

    assert "from decimal import Decimal" in text
    assert "F.lit(Decimal('12.34'))" in text


def test_lag_accepts_decimal_defaults_and_imports_them_in_generated_modules() -> None:
    recipe = cast(PySparkExecutionPlan, _compile(DecimalWindowDefault).lowered)
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

    assert (integer == "one").data == {
        "comparison_problem": "Comparison requires compatible Structure expression types"
    }
    assert integer.null_safe_eq("one").data == {
        "comparison_problem": "Comparison requires compatible Structure expression types"
    }
    with pytest.raises(TypeError, match="compatible with its expression type"):
        integer.isin(1, "one")
    assert (integer == object()).data == {
        "comparison_problem": "Comparison requires compatible Structure expression types"
    }
    assert (_expression(types.boolean(), nullable=False) > False).data == {
        "comparison_problem": "Ordering comparisons require orderable Structure expression types"
    }
    assert (_expression(types.map(types.string(), types.string()), nullable=False) == None).data == {  # noqa: E711
        "comparison_problem": "Comparison requires compatible Structure expression types"
    }

    with pytest.raises(StructureCompileError) as raised:
        _compile(IncompatibleComparison)

    assert raised.value.diagnostic.code == "DSL-E0402"
    with pytest.raises(StructureCompileError) as raised:
        _compile(IncompatibleComparisonFilter)

    assert raised.value.diagnostic.code == "DSL-E0402"

    assert (_expression(types.date(), nullable=False) < datetime.datetime(2026, 7, 15)).type is not None


@pytest.mark.parametrize("helper", [lower, trim, upper])
def test_string_normalizers_require_and_return_string_expressions(helper) -> None:
    normalized = helper(_expression(types.string(), nullable=False))

    assert normalized.type is not None and normalized.type.name == "string"
    assert normalized.nullable is False
    with pytest.raises(TypeError, match=r"requires a String Structure expression"):
        helper(1)


@pytest.mark.parametrize("method", ["startswith", "endswith"])
def test_string_boundary_predicates_require_string_literals_and_preserve_nullability(method: str) -> None:
    nullable = _expression(types.string(), nullable=True)
    predicate = getattr(nullable, method)("order-")

    assert predicate.kind == method
    assert predicate.type is not None and predicate.type.name == "boolean"
    assert predicate.nullable is True
    with pytest.raises(TypeError, match=rf"{method}\(\.\.\.\) requires a string literal"):
        getattr(nullable, method)(1)


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


@pytest.mark.parametrize("unit", ["year", "YYYY", "quarter", "mon", "dd", "hour", "microsecond"])
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
    assert event_time_between(nullable_timestamp, datetime.datetime(2026, 7, 13), upper="1 hour").nullable is True


def test_reflected_boolean_operators_preserve_operand_order() -> None:
    expression = _expression(types.boolean(), nullable=True)

    conjunction = True & expression
    disjunction = False | expression

    assert conjunction.kind == "and"
    assert conjunction.args[0].kind == "literal"
    assert conjunction.args[1] is expression
    assert conjunction.nullable is True
    assert disjunction.kind == "or"
    assert disjunction.args[0].kind == "literal"
    assert disjunction.args[1] is expression
    assert disjunction.nullable is True


def test_event_time_between_can_be_negated_symbolically() -> None:
    timestamp = _expression(types.timestamp(), nullable=False)

    predicate = event_time_between(timestamp, timestamp, upper="1 hour")
    negated = ~predicate

    assert negated.kind == "not"
    assert negated.type is not None and negated.type.name == "boolean"
    assert negated.nullable is False
    assert negated.args[0] is predicate


@pytest.mark.parametrize(
    "expression",
    [
        lambda: _expression(types.string(), nullable=False) & True,
        lambda: _expression(types.boolean(), nullable=False) | 1,
        lambda: ~_expression(types.string(), nullable=False),
        lambda: True & _expression(types.string(), nullable=False),
        lambda: 1 | _expression(types.boolean(), nullable=False),
    ],
)
def test_logical_operators_require_boolean_operands(expression) -> None:
    with pytest.raises(TypeError, match="Boolean Structure expression"):
        expression()


def test_nullable_negation_cannot_fill_a_non_nullable_output() -> None:
    with pytest.raises(StructureCompileError) as raised:
        _compile(NullableNegation)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"
