from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.operations import OperationCardinality
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def _body(transform) -> PySparkStepBody:
    compiled = Compiler.frontend.compile()(transform, materialize_schemas=False)
    analysis = cast(Any, compiled.analysis)
    return cast(PySparkStepBody, analysis.steps[0].plugin_body)


def _recipe(transform) -> PySparkExecutionPlan:
    compiled = Compiler.frontend.compile()(transform, materialize_schemas=False)
    return cast(PySparkExecutionPlan, compiled.lowered)


class Tick(Schema):
    series = string(nullable=False)
    index = long(nullable=False)
    increment = long(nullable=False)


class FibonacciState(Schema):
    previous = long(nullable=False)
    current = long(nullable=False)


class Fibonacci(Schema):
    series = string(nullable=False)
    index = long(nullable=False)
    value = long(nullable=False)


def test_scan_captures_initial_state_transition_and_timeline_keys() -> None:
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
                    current=state.previous + state.current + row.increment,
                ),
            )
            return Fibonacci(series=tick.series, index=tick.index, value=state.previous)

    operation = _body(FibonacciFromTimeline).operations[0]
    planned = operation.ordered_timeline_scan
    projection = {assignment.field.name: assignment.expression for assignment in _body(FibonacciFromTimeline).projection}

    assert operation.cardinality is OperationCardinality.ROW_PRESERVING
    assert planned is not None
    assert planned.row_scope == "ticks"
    assert planned.state_schema is FibonacciState
    assert planned.max_rows == 10_000
    assert tuple(name for name, _ in planned.initial) == ("previous", "current")
    assert tuple(name for name, _ in planned.transition) == ("previous", "current")
    partition_data = cast(dict[str, object], planned.partition_by[0].data)
    order_data = cast(dict[str, object], planned.order_by[0].data)
    value_data = cast(dict[str, object], projection["value"].data)

    assert partition_data["field"] == "series"
    assert order_data["field"] == "index"
    assert value_data["scope"] == "__scan"

    lowered = _recipe(FibonacciFromTimeline).steps[0].operations[0].ordered_timeline_scan
    assert lowered is not None
    assert lowered.scope == "__scan"
    assert lowered.state_schema is FibonacciState
    assert tuple(name for name, _ in lowered.transition) == ("previous", "current")


def test_scan_generated_source_uses_public_group_fold_expand_shape() -> None:
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
                    current=state.previous + state.current + row.increment,
                ),
            )
            return Fibonacci(series=tick.series, index=tick.index, value=state.previous)

    step = _recipe(FibonacciFromTimeline).steps[0]
    text = render_pyspark_step(step, current="ticks", sources={"ticks": "ticks"})

    assert "F.assert_true" in text
    assert ".groupBy(" in text
    assert "F.sort_array(F.collect_list(F.struct(" in text
    assert "F.aggregate(" in text
    assert "lambda acc, item:" in text
    assert "F.posexplode(" in text
    assert ".crossJoin(" in text
    assert "__payload" in text
    assert "__state" in text
    assert "udf" not in text.lower()
    assert ".rdd" not in text
    assert "toPandas" not in text
    assert ".collect(" not in text


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"max_rows": 0}, r"scan\(max_rows=\.\.\.\) requires a positive integer literal"),
        ({"ties": cast(Any, "panic")}, r'scan\(\.\.\.\) ties= must be one of "error"'),
        ({"initial": object()}, r"scan\(initial=\.\.\.\) requires a fully populated Schema state instance"),
    ],
)
def test_scan_rejects_invalid_public_arguments(options: dict[str, object], message: str) -> None:
    @transform
    class BadScan(Transform):
        ticks = input(Tick)
        values = output(Fibonacci)

        def calculate(self, tick: Tick) -> Fibonacci:
            args = {
                "initial": FibonacciState(previous=0, current=1),
                "partition_by": tick.series,
                "order_by": tick.index,
                "max_rows": 10,
                "step": lambda state, row: FibonacciState(previous=state.current, current=state.previous),
            }
            args.update(options)
            state = scan(**cast(Any, args))
            return Fibonacci(series=tick.series, index=tick.index, value=state.previous)

    with pytest.raises((StructureCompileError, TypeError), match=message):
        _body(BadScan)


def test_scan_rejects_incomplete_initial_state() -> None:
    @transform
    class BadScan(Transform):
        ticks = input(Tick)
        values = output(Fibonacci)

        def calculate(self, tick: Tick) -> Fibonacci:
            state = scan(
                initial=FibonacciState(previous=0),
                partition_by=tick.series,
                order_by=tick.index,
                max_rows=10,
                step=lambda state, row: FibonacciState(previous=state.current, current=state.previous),
            )
            return Fibonacci(series=tick.series, index=tick.index, value=state.previous)

    with pytest.raises(StructureCompileError, match="must populate every state field; missing current"):
        _body(BadScan)


def test_scan_rejects_wrong_callback_return_schema() -> None:
    @transform
    class BadScan(Transform):
        ticks = input(Tick)
        values = output(Fibonacci)

        def calculate(self, tick: Tick) -> Fibonacci:
            state = scan(
                initial=FibonacciState(previous=0, current=1),
                partition_by=tick.series,
                order_by=tick.index,
                max_rows=10,
                step=lambda state, row: Fibonacci(value=state.previous, series=row.series, index=row.index),
            )
            return Fibonacci(series=tick.series, index=tick.index, value=state.previous)

    with pytest.raises(StructureCompileError, match="scan\\(step=\\.\\.\\.\\) must return FibonacciState"):
        _body(BadScan)


def test_scan_rejects_order_descriptors_for_first_release() -> None:
    @transform
    class BadScan(Transform):
        ticks = input(Tick)
        values = output(Fibonacci)

        def calculate(self, tick: Tick) -> Fibonacci:
            state = scan(
                initial=FibonacciState(previous=0, current=1),
                partition_by=tick.series,
                order_by=tick.index.desc(),
                max_rows=10,
                step=lambda state, row: FibonacciState(previous=state.current, current=state.previous),
            )
            return Fibonacci(series=tick.series, index=tick.index, value=state.previous)

    with pytest.raises(StructureCompileError, match="requires ascending unordered expressions"):
        _body(BadScan)
