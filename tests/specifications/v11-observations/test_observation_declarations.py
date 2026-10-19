from typing import cast

import pytest

from structure import Schema, Transform, observation, transform
from structure.plugin.pyspark import RowScope, coalesce, count, long, observe, sum
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import PySparkSymbolicContext


class Metrics(Schema):
    rows = long(nullable=False)


@transform
class Measured(Transform):
    quality = observation(Metrics)


class OtherMetrics(Schema):
    rows = long(nullable=False)


def test_observation_declaration_is_named_and_inherited():
    class Child(Measured):
        pass

    assert Measured._structure_observations["quality"].schema is Metrics
    assert Child._structure_observations["quality"] is Measured._structure_observations["quality"]
    assert Measured.quality.name == "quality"


def test_observe_records_typed_metrics_without_aggregating_row_output():
    context = PySparkSymbolicContext(step="measure")
    context.observation_declarations = (Measured.quality,)
    context.default_project_source = "source"
    with context:
        result = observe(Metrics(rows=count()))

    assert result == "source"
    assert context.aggregate_requested is False
    assert len(context.operations) == 1
    assert context.operations[0].kind == "observation"
    assert context.operations[0].observation is not None
    assert context.operations[0].observation.name == "quality"
    assert context.operations[0].observation.schema is Metrics


def test_observe_rejects_ambiguous_schema_inference():
    first = observation(Metrics)
    second = observation(Metrics)
    first.__set_name__(Measured, "first")
    second.__set_name__(Measured, "second")
    context = PySparkSymbolicContext(step="measure")
    context.observation_declarations = (first, second)
    context.default_project_source = "source"
    with context, pytest.raises(TypeError, match="Multiple observations"):
        observe(Metrics(rows=count()))


def test_observe_requires_exact_declared_schema():
    context = PySparkSymbolicContext(step="measure")
    context.observation_declarations = (Measured.quality,)
    context.default_project_source = "source"
    with context, pytest.raises(TypeError, match="No observation is declared"):
        observe(OtherMetrics(rows=count()))


def test_observe_accepts_an_explicit_reference_for_a_shared_schema():
    first = observation(Metrics)
    second = observation(Metrics)
    first.__set_name__(Measured, "first")
    second.__set_name__(Measured, "second")
    context = PySparkSymbolicContext(step="measure")
    context.observation_declarations = (first, second)
    context.default_project_source = "source"
    with context:
        observe(second, Metrics(rows=count()))

    assert context.operations[0].observation is not None
    assert context.operations[0].observation.name == "second"


def test_observe_rejects_bare_row_fields_and_nullability_mismatches():
    class RequiredSum(Schema):
        total = long(nullable=False)

    required_sum = observation(RequiredSum)
    required_sum.__set_name__(Measured, "required_sum")
    row = RowScope(name="order", schema=Metrics)
    context = PySparkSymbolicContext(step="measure")
    context.observation_declarations = (required_sum,)
    context.default_project_source = "source"
    with context, pytest.raises(TypeError, match="field reference"):
        observe(RequiredSum(total=row.rows))

    context = PySparkSymbolicContext(step="measure")
    context.observation_declarations = (required_sum,)
    context.default_project_source = "source"
    with context, pytest.raises(TypeError, match="declare it nullable or add a fallback"):
        observe(RequiredSum(total=sum(row.rows)))


def test_observation_sum_nullability_can_be_fixed_with_coalesce():
    class DefaultedSum(Schema):
        total = long(nullable=False)

    declaration = observation(DefaultedSum)
    declaration.__set_name__(Measured, "defaulted")
    row = RowScope(name="order", schema=Metrics)
    context = PySparkSymbolicContext(step="measure")
    context.observation_declarations = (declaration,)
    context.default_project_source = "source"
    with context:
        observe(DefaultedSum(total=coalesce(sum(row.rows), 0)))

    assert context.operations[0].observation is not None


def test_observation_rejects_a_pending_row_or_grouped_aggregate():
    row = RowScope(name="order", schema=Metrics)
    context = PySparkSymbolicContext(step="measure")
    context.observation_declarations = (Measured.quality,)
    context.default_project_source = "source"
    with context:
        sum(row.rows)
        with pytest.raises(TypeError, match="cannot share a step"):
            observe(Metrics(rows=count()))

    context = PySparkSymbolicContext(step="measure")
    context.observation_declarations = (Measured.quality,)
    context.default_project_source = "source"
    context.aggregate_keys = (("key", cast(Expression, row.rows)),)
    with context, pytest.raises(TypeError, match="cannot share a step"):
        observe(Metrics(rows=count()))
