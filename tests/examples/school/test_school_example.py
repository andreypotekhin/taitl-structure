from typing import Any, cast

import pytest

from examples.school.transforms.algebra import EvaluateAlgebra
from examples.school.transforms.matrices import InvertMatrices, MultiplyMatrices, MultiplyMatrixVector
from examples.school.transforms.sequences import Fibonacci, PrimeNumbers
from examples.school.transforms.series import EAsSeries, Ln2AsSeries, PiAsSeries
from examples.school.transforms.vectors import EvaluateVectors
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark.dsl.operations import OperationCardinality
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def test_school_example_transforms_compile() -> None:
    for transform in (
        EvaluateAlgebra,
        EvaluateVectors,
        MultiplyMatrices,
        MultiplyMatrixVector,
        InvertMatrices,
        Fibonacci,
        PrimeNumbers,
        PiAsSeries,
        EAsSeries,
        Ln2AsSeries,
    ):
        Compiler.frontend.compile()(transform, materialize_schemas=False)


def test_school_iterable_fibonacci_compiles_when_plugin_is_installed() -> None:
    pytest.importorskip("structure_iterable")
    from examples.school.transforms.iterable import IterableFibonacci

    Compiler.frontend.compile()(IterableFibonacci, materialize_schemas=False)


def test_school_fibonacci_uses_ordered_timeline_scan() -> None:
    body = _body(Fibonacci)
    operation = body.operations[0]
    planned = operation.ordered_timeline_scan
    projection = {assignment.field.name: assignment.expression for assignment in body.projection}

    assert operation.cardinality is OperationCardinality.ROW_PRESERVING
    assert planned is not None
    assert planned.row_scope == "ticks"
    assert planned.max_rows == 10_000
    assert tuple(name for name, _ in planned.initial) == ("previous", "current")
    assert tuple(name for name, _ in planned.transition) == ("previous", "current")
    assert cast(dict[str, object], planned.partition_by[0].data)["value"] == 1
    assert cast(dict[str, object], planned.order_by[0].data)["field"] == "index"
    assert cast(dict[str, object], projection["value"].data)["scope"] == "__scan"


def test_school_prime_numbers_uses_ordered_timeline_scan() -> None:
    body = _body(PrimeNumbers)
    operation = body.operations[0]
    planned = operation.ordered_timeline_scan
    projection = {assignment.field.name: assignment.expression for assignment in body.projection}

    assert operation.cardinality is OperationCardinality.ROW_PRESERVING
    assert planned is not None
    assert planned.row_scope == "ticks"
    assert planned.max_rows == 10_000
    assert tuple(name for name, _ in planned.initial) == ("primes", "current")
    assert tuple(name for name, _ in planned.transition) == ("primes", "current")
    assert cast(dict[str, object], planned.partition_by[0].data)["value"] == 1
    assert cast(dict[str, object], planned.order_by[0].data)["field"] == "index"
    assert cast(dict[str, object], projection["prime"].data)["scope"] == "__scan"


def _body(transform) -> PySparkStepBody:
    compiled = Compiler.frontend.compile()(transform, materialize_schemas=False)
    analysis = cast(Any, compiled.analysis)
    return cast(PySparkStepBody, analysis.steps[0].plugin_body)
