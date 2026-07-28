from __future__ import annotations

from math import e, log, pi
from typing import cast

import pytest
from integration.pyspark.support.backend_matrix import session
from integration.pyspark.support.rows import rows, single

from examples.school.transforms.sequences import Fibonacci
from examples.school.transforms.series import EAsSeries, Ln2AsSeries, PiAsSeries

pytestmark = pytest.mark.integration


def test_school_fibonacci_runs_independent_partitions(spark) -> None:
    ticks = spark.createDataFrame(
        [("A", index) for index in range(5)] + [("B", index) for index in range(4)],
        _tick_schema(),
    )

    result = Fibonacci(ticks=ticks).run(session(spark, execution_mode="online")).result

    assert rows(result, "series", "index") == [
        {"series": "A", "index": 0, "value": 0},
        {"series": "A", "index": 1, "value": 1},
        {"series": "A", "index": 2, "value": 1},
        {"series": "A", "index": 3, "value": 2},
        {"series": "A", "index": 4, "value": 3},
        {"series": "B", "index": 0, "value": 0},
        {"series": "B", "index": 1, "value": 1},
        {"series": "B", "index": 2, "value": 1},
        {"series": "B", "index": 3, "value": 2},
    ]


def test_school_series_examples_converge(spark) -> None:
    ticks = spark.createDataFrame([("calc", index) for index in range(1_000)], _tick_schema())

    pi_value = _last_value(PiAsSeries(ticks=ticks).run(session(spark, execution_mode="online")).result)
    e_value = _last_value(EAsSeries(ticks=ticks).run(session(spark, execution_mode="online")).result)
    ln2_value = _last_value(Ln2AsSeries(ticks=ticks).run(session(spark, execution_mode="online")).result)

    assert abs(pi_value - pi) < 0.002
    assert abs(e_value - e) < 0.000001
    assert abs(ln2_value - log(2)) < 0.001


def _last_value(frame) -> float:
    return cast(float, single(frame, lambda row: row["index"] == 999)["value"])


def _tick_schema() -> str:
    return "series string, index long"
