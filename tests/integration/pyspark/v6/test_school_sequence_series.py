from __future__ import annotations

from math import e, log, pi
from typing import cast

import pytest
from integration.pyspark.support.backend_matrix import session
from integration.pyspark.support.rows import rows, single

from examples.school.transforms.sequences import Fibonacci, PrimeNumbers
from examples.school.transforms.series import EAsSeries, Ln2AsSeries, PiAsSeries

pytestmark = pytest.mark.integration


def test_school_fibonacci_runs_sequence(spark) -> None:
    ticks = spark.createDataFrame([(index,) for index in range(5)], _tick_schema())

    result = Fibonacci(ticks=ticks).run(session(spark, execution_mode="online")).result

    assert rows(result, "index") == [
        {"index": 0, "value": 0},
        {"index": 1, "value": 1},
        {"index": 2, "value": 1},
        {"index": 3, "value": 2},
        {"index": 4, "value": 3},
    ]


def test_school_prime_numbers_runs_sequence(spark) -> None:
    ticks = spark.createDataFrame([(index,) for index in range(10)], _tick_schema())

    result = PrimeNumbers(ticks=ticks).run(session(spark, execution_mode="online")).result

    assert rows(result, "index") == [
        {"index": 0, "prime": 2},
        {"index": 1, "prime": 3},
        {"index": 2, "prime": 5},
        {"index": 3, "prime": 7},
        {"index": 4, "prime": 11},
        {"index": 5, "prime": 13},
        {"index": 6, "prime": 17},
        {"index": 7, "prime": 19},
        {"index": 8, "prime": 23},
        {"index": 9, "prime": 29},
    ]


def test_school_series_examples_converge(spark) -> None:
    ticks = spark.createDataFrame([(index,) for index in range(1_000)], _tick_schema())

    pi_value = _last_value(PiAsSeries(ticks=ticks).run(session(spark, execution_mode="online")).result)
    e_value = _last_value(EAsSeries(ticks=ticks).run(session(spark, execution_mode="online")).result)
    ln2_value = _last_value(Ln2AsSeries(ticks=ticks).run(session(spark, execution_mode="online")).result)

    assert abs(pi_value - pi) < 0.002
    assert abs(e_value - e) < 0.000001
    assert abs(ln2_value - log(2)) < 0.001


def _last_value(frame) -> float:
    return cast(float, single(frame, lambda row: row["index"] == 999)["value"])


def _tick_schema() -> str:
    return "index long"
