from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import *
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import *

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v6.test_ordered_timeline_scan"
PACKAGE = "integration_v6_ordered_scan_generated"


class Tick(Schema):
    series = string(nullable=False)
    index = long(nullable=False)


class FibonacciState(Schema):
    previous = long(nullable=False)
    current = long(nullable=False)


class Fibonacci(Schema):
    series = string(nullable=False)
    index = long(nullable=False)
    value = long(nullable=False)


class NullableFibonacci(Schema):
    series = string(nullable=False)
    index = long(nullable=True)
    value = long(nullable=False)


class WeightedTick(Schema):
    series = string(nullable=False)
    index = long(nullable=False)
    increment = long(nullable=False)


class NullableTick(Schema):
    series = string(nullable=False)
    index = long(nullable=True)


@transform
class FibonacciFromTimeline(Transform):
    ticks = input(Tick)
    values = output(Fibonacci)

    def calculate(self, tick: Tick) -> Fibonacci:
        state = scan(
            initial=FibonacciState(previous=0, current=1),
            partition_by=tick.series,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: FibonacciState(
                previous=state.current,
                current=state.previous + state.current,
            ),
        )
        return Fibonacci(series=tick.series, index=tick.index, value=state.previous)


@transform
class WeightedFibonacciFromTimeline(Transform):
    ticks = input(WeightedTick)
    values = output(Fibonacci)

    def calculate(self, tick: WeightedTick) -> Fibonacci:
        state = scan(
            initial=FibonacciState(previous=0, current=1),
            partition_by=tick.series,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: FibonacciState(
                previous=state.current,
                current=state.previous + state.current + row.increment,
            ),
        )
        return Fibonacci(series=tick.series, index=tick.index, value=state.previous)


@transform
class LimitedFibonacciFromTimeline(Transform):
    ticks = input(Tick)
    values = output(Fibonacci)

    def calculate(self, tick: Tick) -> Fibonacci:
        state = scan(
            initial=FibonacciState(previous=0, current=1),
            partition_by=tick.series,
            order_by=tick.index,
            max_rows=2,
            step=lambda state, row: FibonacciState(
                previous=state.current,
                current=state.previous + state.current,
            ),
        )
        return Fibonacci(series=tick.series, index=tick.index, value=state.previous)


@transform
class NullableOrderFibonacciFromTimeline(Transform):
    ticks = input(NullableTick)
    values = output(NullableFibonacci)

    def calculate(self, tick: NullableTick) -> NullableFibonacci:
        state = scan(
            initial=FibonacciState(previous=0, current=1),
            partition_by=tick.series,
            order_by=tick.index,
            max_rows=10_000,
            step=lambda state, row: FibonacciState(
                previous=state.current,
                current=state.previous + state.current,
            ),
        )
        return NullableFibonacci(series=tick.series, index=tick.index, value=state.previous)


def test_ordered_timeline_scan_runs_online_and_generated(spark, tmp_path) -> None:
    files = render_generated_project(
        FibonacciFromTimeline,
        source_transform=f"{SOURCE_MODULE}.FibonacciFromTimeline",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [Tick, FibonacciState, Fibonacci]},
    )

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_ordered_timeline_scan")
        ticks = spark.createDataFrame(
            [
                ("A", 0),
                ("A", 1),
                ("A", 2),
                ("A", 4),
                ("B", 0),
                ("B", 1),
                ("B", 2),
                ("B", 3),
            ],
            generated_schemas.TICK_SCHEMA,
        )

        assert_online_generated_parity(
            lambda: FibonacciFromTimeline(ticks=ticks).run(session(spark, execution_mode="online")),
            lambda: FibonacciFromTimeline(ticks=ticks).run(
                session(spark, execution_mode="generated", generated_package=PACKAGE)
            ),
        )

        online = FibonacciFromTimeline(ticks=ticks).run(session(spark, execution_mode="online"))["values"]
        assert rows(online, "series", "index") == [
            {"series": "A", "index": 0, "value": 0},
            {"series": "A", "index": 1, "value": 1},
            {"series": "A", "index": 2, "value": 1},
            {"series": "A", "index": 4, "value": 2},
            {"series": "B", "index": 0, "value": 0},
            {"series": "B", "index": 1, "value": 1},
            {"series": "B", "index": 2, "value": 1},
            {"series": "B", "index": 3, "value": 2},
        ]


def test_ordered_timeline_scan_empty_input_keeps_declared_schema(spark, tmp_path) -> None:
    files = render_generated_project(
        FibonacciFromTimeline,
        source_transform=f"{SOURCE_MODULE}.FibonacciFromTimeline",
        generated_package=f"{PACKAGE}_empty",
        source_schema_modules={SOURCE_MODULE: [Tick, FibonacciState, Fibonacci]},
    )

    with generated_project(tmp_path, f"{PACKAGE}_empty", files):
        generated_schemas = importlib.import_module(f"{PACKAGE}_empty.pyspark.schemas.test_ordered_timeline_scan")
        ticks = spark.createDataFrame([], generated_schemas.TICK_SCHEMA)
        result = FibonacciFromTimeline(ticks=ticks).run(session(spark, execution_mode="online"))["values"]

        assert rows(result) == []
        assert result.columns == ["series", "index", "value"]


def test_ordered_timeline_scan_transition_can_read_current_row(spark, tmp_path) -> None:
    files = render_generated_project(
        WeightedFibonacciFromTimeline,
        source_transform=f"{SOURCE_MODULE}.WeightedFibonacciFromTimeline",
        generated_package=f"{PACKAGE}_weighted",
        source_schema_modules={SOURCE_MODULE: [WeightedTick, FibonacciState, Fibonacci]},
    )

    with generated_project(tmp_path, f"{PACKAGE}_weighted", files):
        generated_schemas = importlib.import_module(f"{PACKAGE}_weighted.pyspark.schemas.test_ordered_timeline_scan")
        ticks = spark.createDataFrame(
            [
                ("A", 0, 0),
                ("A", 1, 10),
                ("A", 2, 100),
            ],
            generated_schemas.WEIGHTED_TICK_SCHEMA,
        )

        result = WeightedFibonacciFromTimeline(ticks=ticks).run(session(spark, execution_mode="online"))["values"]

        assert rows(result, "index") == [
            {"series": "A", "index": 0, "value": 0},
            {"series": "A", "index": 1, "value": 1},
            {"series": "A", "index": 2, "value": 1},
        ]


@pytest.mark.parametrize(
    ("transform_type", "schema_name", "data"),
    [
        (FibonacciFromTimeline, "TICK_SCHEMA", [("A", 0), ("A", 0)]),
        (LimitedFibonacciFromTimeline, "TICK_SCHEMA", [("A", 0), ("A", 1), ("A", 2)]),
        (NullableOrderFibonacciFromTimeline, "NULLABLE_TICK_SCHEMA", [("A", None)]),
    ],
)
def test_ordered_timeline_scan_runtime_guards_fail(spark, tmp_path, transform_type, schema_name, data) -> None:
    package = f"{PACKAGE}_{transform_type.__name__.lower()}"
    files = render_generated_project(
        transform_type,
        source_transform=f"{SOURCE_MODULE}.{transform_type.__name__}",
        generated_package=package,
        source_schema_modules={SOURCE_MODULE: [Tick, NullableTick, FibonacciState, Fibonacci, NullableFibonacci]},
    )

    with generated_project(tmp_path, package, files):
        generated_schemas = importlib.import_module(f"{package}.pyspark.schemas.test_ordered_timeline_scan")
        ticks = spark.createDataFrame(data, getattr(generated_schemas, schema_name))
        result = transform_type(ticks=ticks).run(session(spark, execution_mode="online"))["values"]

        with pytest.raises(Exception, match="SCAN-E0801"):
            rows(result)
