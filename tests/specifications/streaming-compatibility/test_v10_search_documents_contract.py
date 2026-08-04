from __future__ import annotations

from typing import cast

from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


def _walk(expression):
    yield expression
    for argument in expression.args:
        yield from _walk(argument)


def test_search_documents_streaming_report_names_each_current_state_blocker() -> None:
    compilation = Compiler.frontend.compile()(SearchDocuments, materialize_schemas=False)
    plan = cast(PySparkExecutionPlan, compilation.lowered)

    report = Compiler.compileability.streaming()(plan, required=True)
    operations = {finding.operation for finding in report.findings}
    steps = {finding.step for finding in report.findings}

    assert report.required is True
    assert report.support.value == "batch_only"
    assert "subset duplicate removal" in operations
    assert "unbounded business-key aggregate" in operations
    assert "rowset join policy" in operations
    assert "select_first_qualified" in operations
    assert "window projection" in operations
    assert any(operation.startswith("stream-stream join ") for operation in operations)
    assert any(step.startswith("retrieved.rank_candidates") for step in steps)
    assert any(step.startswith("reranked.select_query_feedback") for step in steps)
    assert all(stage.operation == "unbounded business-key aggregate" for stage in report.stages)
    assert all(stage.completion_window is None for stage in report.stages)
    assert report.stages


def test_search_documents_captures_exact_query_request_event_time_contract() -> None:
    compilation = Compiler.frontend.compile()(SearchDocuments, materialize_schemas=False)
    plan = cast(PySparkExecutionPlan, compilation.lowered)
    candidate_steps = [
        step
        for step in plan.steps
        if step.name in {"retrieved.select_stored_candidates", "retrieved.select_streamed_candidates"}
    ]

    assert len(candidate_steps) == 2
    for step in candidate_steps:
        watermarks = {
            (operation.watermark.scope, operation.watermark.column, operation.watermark.delay)
            for operation in step.operations
            if operation.watermark is not None
        }
        assert ("query", "requested_at", "10 minutes") in watermarks
        assert ("request", "requested_at", "10 minutes") in watermarks

        filters = [operation.filter for operation in step.operations if operation.filter is not None]
        expressions = [expression for predicate in filters for expression in _walk(predicate)]
        timestamp_equalities = [
            expression
            for expression in expressions
            if expression.kind == "eq"
            and {str(argument.data.get("scope")) for argument in expression.args}
            == {"query", "request"}
            and {str(argument.data.get("field")) for argument in expression.args}
            == {"requested_at"}
        ]
        bounds = [
            expression
            for expression in expressions
            if expression.kind == "event_time_between"
            and (expression.data or {}).get("lower") == "0 seconds"
            and (expression.data or {}).get("upper") == "0 seconds"
        ]
        assert len(timestamp_equalities) == 1
        assert len(bounds) == 1
