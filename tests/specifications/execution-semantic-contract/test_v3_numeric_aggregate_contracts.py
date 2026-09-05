from typing import cast

import pytest

from structure import *
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.types import ArrayType, DecimalType
from structure.plugin.pyspark.dsl.windows import WindowFrame


@pytest.mark.parametrize(
    "call",
    [
        lambda: rolling_sum("amount", partition_by="tenant", order_by="ordered", preceding=1),
        lambda: rolling_avg("amount", partition_by="tenant", order_by="ordered", preceding=1),
        lambda: window_sum("amount", over=window(partition_by="tenant", order_by="ordered")),
        lambda: window_avg("amount", over=window(partition_by="tenant", order_by="ordered")),
    ],
)
def test_numeric_window_helpers_reject_non_numeric_arguments(call) -> None:
    with pytest.raises(TypeError, match="requires a numeric expression"):
        call()


@pytest.mark.parametrize(
    "call",
    [
        lambda: rolling_min(array("amount"), partition_by="tenant", order_by="ordered", preceding=1),
        lambda: rolling_max(array("amount"), partition_by="tenant", order_by="ordered", preceding=1),
        lambda: window_min(array("amount"), over=window(partition_by="tenant", order_by="ordered")),
        lambda: window_max(array("amount"), over=window(partition_by="tenant", order_by="ordered")),
    ],
)
def test_extrema_window_helpers_reject_non_orderable_arguments(call) -> None:
    with pytest.raises(TypeError, match="requires an orderable scalar expression"):
        call()


def test_window_rejects_invalid_frame_objects() -> None:
    with pytest.raises(TypeError, match=r"window\(frame=\.\.\.\) requires rows_between"):
        window(partition_by="tenant", order_by="ordered", frame=cast(WindowFrame, "current row"))


def test_exact_percentile_and_moment_statistics_require_numeric_values() -> None:
    for expression in (percentile(1, 0.5), skewness(1), kurtosis(1)):
        assert expression.type is not None and expression.type.name == "double"
        assert expression.nullable is True

    with pytest.raises(TypeError, match="percentage"):
        percentile(1, 1.1)
    with pytest.raises(TypeError, match="frequency"):
        percentile(1, 0.5, frequency=0)


def test_aggregate_aliases_preserve_typed_numeric_contracts() -> None:
    required_number = Expression(kind="number", type=types.long(), nullable=False)
    required_boolean = Expression(kind="predicate", type=types.boolean(), nullable=False)

    counted = count_if(required_boolean)
    assert counted.type is not None and counted.type.name == "long"
    for expression in (
        median(required_number),
        stddev_pop(required_number),
        stddev_samp(required_number),
        var_pop(required_number),
        var_samp(required_number),
    ):
        assert expression.type is not None and expression.type.name == "double"
        assert expression.nullable is True

    with pytest.raises(TypeError, match=r"count_if\(\.\.\.\) requires a Boolean"):
        count_if(required_number)
    with pytest.raises(TypeError, match=r"median\(\.\.\.\) requires a numeric"):
        median("not numeric")


def test_mode_preserves_candidate_type_and_deterministic_tie_contract() -> None:
    required_text = Expression(kind="text", type=types.string(), nullable=False)
    nullable_text = Expression(kind="nullable_text", type=types.string(), nullable=True)

    default_mode = mode(nullable_text)
    deterministic_mode = mode(required_text, deterministic=True)

    assert default_mode.type is nullable_text.type
    assert default_mode.nullable is True
    assert dict(default_mode.data or {})["deterministic"] is False
    assert deterministic_mode.type is required_text.type
    assert deterministic_mode.nullable is True
    assert dict(deterministic_mode.data or {})["deterministic"] is True

    with pytest.raises(TypeError, match="deterministic must be a Boolean"):
        mode(required_text, deterministic=cast(bool, "yes"))
    with pytest.raises(TypeError, match="requires an orderable scalar expression"):
        mode(array("category"), deterministic=True)


def test_advanced_aggregate_helpers_preserve_result_contracts() -> None:
    required_text = Expression(kind="text", type=types.string(), nullable=False)
    nullable_text = Expression(kind="nullable_text", type=types.string(), nullable=True)
    required_number = Expression(kind="number", type=types.long(), nullable=False)

    selected = any_value(nullable_text, ignore_nulls=True)
    collected = array_agg(required_text)
    first_text = first(required_text, ignore_nulls=True)
    last_text = last(nullable_text)
    maximum_text = max_by(required_text, required_number)
    minimum_text = min_by(required_text, required_number)
    multiplied = product(required_number)
    bitwise_results = (bit_and(required_number), bit_or(required_number), bit_xor(required_number))
    regression_results = (
        regr_avgx(required_number, required_number),
        regr_avgy(required_number, required_number),
        regr_count(required_number, required_number),
        regr_intercept(required_number, required_number),
        regr_r2(required_number, required_number),
        regr_slope(required_number, required_number),
        regr_sxx(required_number, required_number),
        regr_sxy(required_number, required_number),
        regr_syy(required_number, required_number),
    )

    assert selected.type is nullable_text.type
    assert selected.nullable is True
    assert dict(selected.data or {})["ignore_nulls"] is True
    assert isinstance(collected.type, ArrayType)
    assert collected.type.element is required_text.type
    assert collected.type.contains_null is False
    assert collected.nullable is False
    assert first_text.type is required_text.type and first_text.nullable is True
    assert last_text.type is nullable_text.type and last_text.nullable is True
    assert maximum_text.type is required_text.type and maximum_text.nullable is True
    assert minimum_text.type is required_text.type and minimum_text.nullable is True
    assert multiplied.type is not None and multiplied.type.name == "double"
    assert multiplied.nullable is True
    assert all(result.type is not None and result.type.name == "long" and result.nullable for result in bitwise_results)
    assert all(result.type is not None and result.type.name == "double" and result.nullable for result in regression_results[:2])
    assert regression_results[2].type is not None and regression_results[2].type.name == "long"
    assert regression_results[2].nullable is False
    assert all(result.type is not None and result.type.name == "double" and result.nullable for result in regression_results[3:])

    with pytest.raises(TypeError, match="ignore_nulls must be a Boolean"):
        any_value(required_text, ignore_nulls=cast(bool, "yes"))
    with pytest.raises(TypeError, match="requires an orderable scalar expression"):
        max_by(required_text, array("ordering"))
    with pytest.raises(TypeError, match="requires a numeric expression"):
        product("not numeric")
    with pytest.raises(TypeError, match="requires an integer or long"):
        bit_xor("not integral")
    with pytest.raises(TypeError, match="requires a numeric expression"):
        sum_distinct("not numeric")
    with pytest.raises(TypeError, match="requires a numeric expression"):
        regr_slope(required_text, required_number)


def test_sum_uses_spark_widened_types_and_filtered_aggregate_nullability() -> None:
    required_integer = Expression(kind="test_integer", type=types.integer(), nullable=False)
    required_float = Expression(kind="test_float", type=types.float(), nullable=False)
    decimal = Expression(kind="test_decimal", type=types.decimal(32, 2), nullable=False)
    frame = window(
        partition_by="tenant",
        order_by="ordered",
        frame=rows_between(preceding(1), preceding(1)),
    )
    current_frame = window(
        partition_by="tenant",
        order_by="ordered",
        frame=rows_between(preceding(1), current_row()),
    )

    integer_sum = sum(required_integer)
    distinct_integer_sum = sum_distinct(required_integer)
    float_sum = sum(required_float)
    decimal_sum = sum(decimal)
    filtered_sum = sum(required_integer, where=True)
    rolling_integer_sum = rolling_sum(
        required_integer,
        partition_by="tenant",
        order_by="ordered",
        preceding=1,
    )
    rolling_decimal_sum = rolling_sum(
        decimal,
        partition_by="tenant",
        order_by="ordered",
        preceding=1,
    )
    windowed_sum = window_sum(required_integer, over=frame)
    windowed_minimum = window_min(required_integer, over=frame)
    windowed_maximum = window_max(required_integer, over=frame)
    aggregate_average = avg(decimal)
    rolling_average = rolling_avg(
        decimal,
        partition_by="tenant",
        order_by="ordered",
        preceding=1,
    )
    windowed_average = window_avg(decimal, over=frame)
    current_window_sum = window_sum(required_integer, over=current_frame)
    current_window_average = window_avg(decimal, over=current_frame)
    current_window_minimum = window_min(required_integer, over=current_frame)
    current_window_maximum = window_max(required_integer, over=current_frame)

    assert integer_sum.type is not None and integer_sum.type.name == "long"
    assert distinct_integer_sum.type is not None and distinct_integer_sum.type.name == "long"
    assert distinct_integer_sum.nullable is False
    assert float_sum.type is not None and float_sum.type.name == "double"
    assert isinstance(decimal_sum.type, DecimalType)
    assert decimal_sum.type.precision == 38
    assert decimal_sum.type.scale == 2
    assert filtered_sum.nullable is True
    assert rolling_integer_sum.type is not None and rolling_integer_sum.type.name == "long"
    assert isinstance(rolling_decimal_sum.type, DecimalType)
    assert rolling_decimal_sum.type.precision == 38
    assert rolling_decimal_sum.type.scale == 2
    assert windowed_sum.type is not None and windowed_sum.type.name == "long"
    assert windowed_sum.nullable is True
    assert windowed_minimum.nullable is True
    assert windowed_maximum.nullable is True
    assert current_window_sum.nullable is False
    assert current_window_average.nullable is False
    assert current_window_minimum.nullable is False
    assert current_window_maximum.nullable is False
    for average in (aggregate_average, rolling_average, windowed_average):
        assert isinstance(average.type, DecimalType)
        assert average.type.precision == 36
        assert average.type.scale == 6


@pytest.mark.parametrize("call", [lambda: sum(1, where=1), lambda: max(1, where="included")])
def test_filtered_aggregates_require_boolean_predicates(call) -> None:
    with pytest.raises(TypeError, match=r"where must be a Boolean expression"):
        call()


@pytest.mark.parametrize(
    "call",
    [
        lambda: window(partition_by="tenant", order_by=array("ordered")),
        lambda: lag(1, partition_by="tenant", order_by=array("ordered")),
        lambda: first_value(1, order_by=array("ordered")),
        lambda: last_value(1, order_by=array("ordered")),
    ],
)
def test_ordered_helpers_reject_collection_ordering_expressions(call) -> None:
    with pytest.raises(TypeError, match="orderable scalar expression"):
        call()


@pytest.mark.parametrize(
    "call",
    [
        lambda: first_value(1, order_by="sequence", ignore_nulls=True),
        lambda: last_value(1, order_by="sequence", ignore_nulls=True),
    ],
)
def test_ordered_aggregate_values_reject_window_only_ignore_nulls(call) -> None:
    with pytest.raises(TypeError, match=r"ignore_nulls=True\) requires over"):
        call()


@pytest.mark.parametrize(
    "call",
    [
        lambda: rank(partition_by="tenant", order_by="ordered", descending=cast(bool, 1)),
        lambda: rolling_sum(
            1,
            partition_by="tenant",
            order_by="ordered",
            preceding=1,
            descending=cast(bool, "reverse"),
        ),
        lambda: nth_value(
            1,
            1,
            over=window(partition_by="tenant", order_by="ordered"),
            ignore_nulls=cast(bool, 1),
        ),
        lambda: first_value(
            1,
            over=window(partition_by="tenant", order_by="ordered"),
            ignore_nulls=cast(bool, "yes"),
        ),
        lambda: arr_sort_by(array("priority"), lambda item: item, descending=cast(bool, 1)),
    ],
)
def test_ordering_and_null_handling_options_require_booleans(call) -> None:
    with pytest.raises(TypeError, match="must be a Boolean"):
        call()


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (
            lambda: lag(1, partition_by="tenant", order_by="ordered", default="missing"),
            "default must be compatible",
        ),
        (
            lambda: lead(
                1,
                partition_by="tenant",
                order_by="ordered",
                default=Expression(kind="test_default", type=types.long(), nullable=False),
            ),
            "default must be a Python scalar literal",
        ),
    ],
)
def test_lag_and_lead_reject_unlowerable_or_incompatible_defaults(call, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        call()
