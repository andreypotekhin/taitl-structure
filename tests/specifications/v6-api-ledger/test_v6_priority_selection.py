from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin import pyspark
from structure.plugin.pyspark import boolean, integer, select_first_qualified, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.joins import TiePolicy
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class Candidate(Schema):
    request_id = string(nullable=False)
    document_id = string(nullable=False)
    qualified = boolean(nullable=False)
    priority = integer(nullable=False)


class PickCandidate(Transform):
    candidates = input(Candidate)
    selected = output(Candidate)

    def pick(self, candidate: Candidate) -> Candidate:
        selected = select_first_qualified(
            candidate.request_id,
            where=candidate.qualified,
            order_by=candidate.priority.asc(),
            missing="error",
        )
        return Candidate.project(selected)


def test_select_first_qualified_is_public_pyspark_api() -> None:
    assert pyspark.select_first_qualified is select_first_qualified


def test_select_first_qualified_records_compiler_visible_operation() -> None:
    operation = _lowered().steps[0].operations[0]

    assert operation.kind == "select_first_qualified"
    assert operation.relation_priority_selection is not None
    assert len(operation.relation_priority_selection.keys) == 1
    assert operation.relation_priority_selection.missing == "error"
    assert operation.relation_priority_selection.ties is TiePolicy.ERROR


def test_select_first_qualified_renders_public_pyspark_priority_selection() -> None:
    text = render_pyspark_step(_lowered().steps[0], current="candidates", sources={"candidates": "candidates"})

    assert 'candidates_select_first_qualified_0_keys = candidates.select(' in text
    assert 'F.coalesce(F.col("candidate.qualified"), F.lit(False))' in text
    assert (
        'candidates_select_first_qualified_0_eligible = candidates_select_first_qualified_0_eligible.withColumn'
        in text
    )
    assert (
        'F.row_number().over(Window.partitionBy(F.col("candidate.request_id")).'
        'orderBy(F.col("candidate.priority").asc()))'
    ) in text
    assert "REL-E0705: select_first_qualified" in text


def test_select_first_qualified_explain_names_cardinality_and_streaming_status() -> None:
    text = render_explain_report(PickCandidate)

    assert "operations: select_first_qualified(select_one keys=1 missing=error ties=error)" in text
    assert "status: compatible" in text


def test_select_first_qualified_records_traceability_dependencies() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(),
        source_transform="tests.PickCandidate",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    dependency = dependencies["pick.select_first_qualified[0]"]
    assert dependency.sources == ("candidates.request_id", "candidates.qualified", "candidates.priority")
    assert dependency.operation == "select_first_qualified"
    assert dependency.detail["keys"] == 1
    assert dependency.detail["missing"] == "error"
    assert dependency.detail["diagnostic"] == "REL-E0705"


def test_select_first_qualified_rejects_invalid_arguments() -> None:
    class MissingKey(Transform):
        candidates = input(Candidate)
        selected = output(Candidate)

        def pick(self, candidate: Candidate) -> Candidate:
            return Candidate.project(
                select_first_qualified(where=candidate.qualified, order_by=candidate.priority)
            )

    class NonFieldKey(Transform):
        candidates = input(Candidate)
        selected = output(Candidate)

        def pick(self, candidate: Candidate) -> Candidate:
            return Candidate.project(
                select_first_qualified(candidate.priority + 1, where=candidate.qualified, order_by=candidate.priority)
            )

    class NonBooleanEligibility(Transform):
        candidates = input(Candidate)
        selected = output(Candidate)

        def pick(self, candidate: Candidate) -> Candidate:
            return Candidate.project(
                select_first_qualified(candidate.request_id, where=candidate.priority, order_by=candidate.priority)
            )

    class BadMissingPolicy(Transform):
        candidates = input(Candidate)
        selected = output(Candidate)

        def pick(self, candidate: Candidate) -> Candidate:
            return Candidate.project(
                select_first_qualified(
                    candidate.request_id,
                    where=candidate.qualified,
                    order_by=candidate.priority,
                    missing="skip",
                )
            )

    with pytest.raises(TypeError, match="at least one declared key"):
        Compiler.frontend.compile()(MissingKey, materialize_schemas=False)
    with pytest.raises(TypeError, match="declared field references"):
        Compiler.frontend.compile()(NonFieldKey, materialize_schemas=False)
    with pytest.raises(TypeError, match="Boolean expression"):
        Compiler.frontend.compile()(NonBooleanEligibility, materialize_schemas=False)
    with pytest.raises(TypeError, match="'allow' or 'error'"):
        Compiler.frontend.compile()(BadMissingPolicy, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(PickCandidate, materialize_schemas=False).lowered,
    )
