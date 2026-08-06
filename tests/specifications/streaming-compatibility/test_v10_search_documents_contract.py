from __future__ import annotations

from typing import Any, cast

import pytest

from examples.search.adoption import (
    REQUIRED_SNAPSHOT_INPUTS,
    SEARCH_STREAMING_CONTRACTS_ENABLED,
    SearchDocumentsRunContract,
    SearchFiniteTopKContract,
)
from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


def _walk(expression):
    yield expression
    for argument in expression.args:
        yield from _walk(argument)


def test_search_documents_design_gated_streaming_is_disabled_for_delivery() -> None:
    compilation = Compiler.frontend.compile()(SearchDocuments, materialize_schemas=False)
    plan = cast(PySparkExecutionPlan, compilation.lowered)

    report = Compiler.compileability.streaming()(plan, required=False)

    assert SEARCH_STREAMING_CONTRACTS_ENABLED is False
    assert report.required is False
    assert all(
        finding.operation == "subset duplicate removal"
        for finding in report.findings
    )
    assert not any(
        finding.operation in {"select_first_qualified", "window projection", "stateful streaming composition"}
        or finding.operation.startswith("stream-stream join")
        for finding in report.findings
    )
    assert report.stages == ()


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
            and {str(argument.data.get("scope")) for argument in expression.args} == {"query", "request"}
            and {str(argument.data.get("field")) for argument in expression.args} == {"requested_at"}
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


def test_search_documents_run_contract_requires_immutable_append_handoff() -> None:
    """The caller-owned run binds one snapshot, checkpoint, trigger, and finality policy."""

    contract = SearchDocumentsRunContract(
        snapshot_id="search-snapshot-v1",
        snapshot_inputs=REQUIRED_SNAPSHOT_INPUTS,
        sink_identity="search-results-parquet",
        checkpoint_identity="search-documents-v1",
        trigger="availableNow",
        output_mode="append",
        event_time_field="requested_at",
        completion_window="10 minutes",
        refresh_restart_policy="new_run_on_snapshot_refresh",
        finality_policy="append_final_no_revisions",
        downstream_materialization="persist final results before serving",
    )

    contract.validate()


def test_search_documents_run_contract_rejects_incomplete_snapshot() -> None:
    """A run cannot silently mix or omit serving snapshot inputs."""

    if not SEARCH_STREAMING_CONTRACTS_ENABLED:
        pytest.skip("SearchDocuments design-gated contracts are disabled for delivery")

    contract = SearchDocumentsRunContract(
        snapshot_id="search-snapshot-v1",
        snapshot_inputs=("index", "score_cache", "feedback", "policy"),
        sink_identity="search-results-parquet",
        checkpoint_identity="search-documents-v1",
        trigger="availableNow",
        output_mode="append",
        event_time_field="requested_at",
        completion_window="10 minutes",
        refresh_restart_policy="new_run_on_snapshot_refresh",
        finality_policy="append_final_no_revisions",
        downstream_materialization="persist final results before serving",
    )

    with pytest.raises(ValueError, match="SEARCH-RUN-E1001"):
        contract.validate()


def test_search_documents_run_contract_rejects_non_append_or_revision_policy() -> None:
    """The handoff refuses output or refresh policies that permit revisions."""

    if not SEARCH_STREAMING_CONTRACTS_ENABLED:
        pytest.skip("SearchDocuments design-gated contracts are disabled for delivery")

    contract = SearchDocumentsRunContract(
        snapshot_id="search-snapshot-v1",
        snapshot_inputs=REQUIRED_SNAPSHOT_INPUTS,
        sink_identity="search-results-parquet",
        checkpoint_identity="search-documents-v1",
        trigger="processingTime:5 minutes",
        output_mode=cast(Any, "update"),
        event_time_field=cast(Any, "event_time"),
        completion_window="10 minutes",
        refresh_restart_policy="new_run_on_snapshot_refresh",
        finality_policy="append_final_no_revisions",
        downstream_materialization="persist final results before serving",
    )

    with pytest.raises(ValueError, match="SEARCH-RUN-E1002"):
        contract.validate()


def test_search_documents_run_contract_rejects_non_finite_completion_window() -> None:
    """A run handoff must identify a positive finite completion interval."""

    if not SEARCH_STREAMING_CONTRACTS_ENABLED:
        pytest.skip("SearchDocuments design-gated contracts are disabled for delivery")

    contract = SearchDocumentsRunContract(
        snapshot_id="search-snapshot-v1",
        snapshot_inputs=REQUIRED_SNAPSHOT_INPUTS,
        sink_identity="search-results-parquet",
        checkpoint_identity="search-documents-v1",
        trigger="availableNow",
        output_mode="append",
        event_time_field="requested_at",
        completion_window="eventually",
        refresh_restart_policy="new_run_on_snapshot_refresh",
        finality_policy="append_final_no_revisions",
        downstream_materialization="persist final results before serving",
    )

    with pytest.raises(ValueError, match="SEARCH-RUN-E1001"):
        contract.validate()


@pytest.mark.parametrize(
    ("stage", "retained_bound"),
    [("candidate_admission", 1000), ("overlap_narrowing", 100)],
)
def test_search_documents_finite_top_k_contract_is_bounded_and_deterministic(stage: str, retained_bound: int) -> None:
    """Candidate and overlap stages declare finite state and deterministic ties."""

    contract = SearchFiniteTopKContract(
        stage=cast(Any, stage),
        retained_bound=retained_bound,
        grouping_key=("query_id",),
        order_keys=("score desc", "document_id asc"),
        tie_policy="score_desc_document_id_asc",
        event_time_field="requested_at",
        watermark_delay="10 minutes",
        completion_window="10 minutes",
        output_mode="append",
        snapshot_id="search-snapshot-v1",
        state_identity=f"search-{stage}-v1",
        restart_policy="same_checkpoint_same_snapshot",
    )

    contract.validate()


def test_search_documents_finite_top_k_contract_rejects_unbounded_or_nondeterministic_shape() -> None:
    """A top-K contract cannot silently accept a global rank window or unstable ties."""

    if not SEARCH_STREAMING_CONTRACTS_ENABLED:
        pytest.skip("SearchDocuments design-gated contracts are disabled for delivery")

    contract = SearchFiniteTopKContract(
        stage="candidate_admission",
        retained_bound=100,
        grouping_key=("query_id",),
        order_keys=("score desc",),
        tie_policy="score_desc_document_id_asc",
        event_time_field="requested_at",
        watermark_delay="10 minutes",
        completion_window="10 minutes",
        output_mode="append",
        snapshot_id="search-snapshot-v1",
        state_identity="search-candidate-v1",
        restart_policy="same_checkpoint_same_snapshot",
    )

    with pytest.raises(ValueError, match="SEARCH-TOPK-E1011"):
        contract.validate()


def test_search_documents_finite_top_k_contract_requires_query_grouping_and_positive_durations() -> None:
    """A top-K stage must be bounded by a query group and finite time declarations."""

    if not SEARCH_STREAMING_CONTRACTS_ENABLED:
        pytest.skip("SearchDocuments design-gated contracts are disabled for delivery")

    contract = SearchFiniteTopKContract(
        stage="candidate_admission",
        retained_bound=1000,
        grouping_key=("document_id",),
        order_keys=("score desc", "document_id asc"),
        tie_policy="score_desc_document_id_asc",
        event_time_field="requested_at",
        watermark_delay="0 seconds",
        completion_window="10 minutes",
        output_mode="append",
        snapshot_id="search-snapshot-v1",
        state_identity="search-candidate-v1",
        restart_policy="same_checkpoint_same_snapshot",
    )

    with pytest.raises(ValueError, match="SEARCH-TOPK-E1010"):
        contract.validate()
