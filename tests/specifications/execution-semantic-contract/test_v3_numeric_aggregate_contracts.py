from typing import cast

import pytest

from structure import *
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.types import DecimalType
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
