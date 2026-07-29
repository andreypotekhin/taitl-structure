from __future__ import annotations

import difflib
from typing import cast

import pytest
from helpers.example_projects import (
    ROOT,
    expected_school_iterable_generated,
    expected_search_generated,
    expected_security_generated,
    expected_stocks_generated,
    expected_store_generated,
    expected_streams_generated,
    render_school_iterable_example,
    render_search_example,
    render_security_example,
    render_stocks_example,
    render_store_example,
    render_streams_example,
)

from structure import StructureConfig, Transform
from structure.core.cli.commands.DiscoverStructureProject import DiscoverStructureProject
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model import TransformPlan


@pytest.mark.parametrize(
    ("actual", "expected"),
    [
        (render_store_example, expected_store_generated),
        (render_streams_example, expected_streams_generated),
        (render_stocks_example, expected_stocks_generated),
        (render_security_example, expected_security_generated),
        (render_search_example, expected_search_generated),
        (render_school_iterable_example, expected_school_iterable_generated),
    ],
)
def test_example_generated_output_matches_golden_files(actual, expected) -> None:
    actual = actual()
    expected = expected()

    assert set(actual) == set(expected), _paths_diff(actual, expected)
    for path in expected:
        assert actual[path] == expected[path], _text_diff(path, expected[path], actual[path])


def test_search_scoring_subpackage_transform_is_discovered_and_compiled() -> None:
    """A nested transform module is a normal source-discovery entrypoint."""

    config = StructureConfig.resolve(project_root=ROOT, source_roots=["examples/search"])
    project = DiscoverStructureProject()(config)
    scoring = next(
        transform
        for transform in project.transforms
        if transform.__module__ == "examples.search.transforms.scoring.Scoring" and transform.__name__ == "Scoring"
    )

    Compiler.frontend.compile()(scoring, config=config, materialize_schemas=False)


def test_search_experiments_replace_production_stages() -> None:
    """Experiment transforms inherit the production graph and replace only their variant stages."""

    from examples.search.transforms.experiments.scoring.scoring001_adjust_bm import Scoring001AdjustBm
    from examples.search.transforms.experiments.searching.search_docs.searching001_adjust_rerank import (
        Searching001AdjustRerankDocuments,
        Searching001AdjustRerankSearchDocuments,
    )
    from examples.search.transforms.scoring.Scoring import Scoring

    scoring = cast(TransformPlan, Compiler.frontend.compile()(Scoring001AdjustBm, materialize_schemas=False).analysis)
    searching = cast(
        TransformPlan,
        Compiler.frontend.compile()(Searching001AdjustRerankSearchDocuments, materialize_schemas=False).analysis,
    )

    assert {step.name.split(".")[0] for step in scoring.steps} == {"overlap", "bm25", "selected"}
    assert Scoring001AdjustBm.bm25.invocation.k1 == 1.35
    assert Scoring001AdjustBm.bm25.invocation.b == 0.70
    assert Scoring001AdjustBm.experiment_id == "scoring001_adjust_bm"
    assert Scoring001AdjustBm.selected is Scoring.selected
    rerank = next(step for step in searching.steps if step.name == "reranked.score_candidates")
    assert rerank.origin is not None
    assert rerank.origin.class_name == Searching001AdjustRerankDocuments.__name__


def test_similarity_public_transform_inherits_its_searching_implementation() -> None:
    """The public transform stays import-stable while its implementation is grouped with search transforms."""

    from examples.search.transforms.searching.search_similarity import SearchSimilarity
    from examples.search.transforms.similarity import Similarity

    assert Similarity.__bases__ == (SearchSimilarity,)


def test_behavior_evaluator_keeps_its_request_to_daily_pipeline_local() -> None:
    """The public evaluator owns all behavior stages for direct IDE navigation."""

    from examples.search.transforms.evaluation.search_docs.behavior.eval_behavior import EvaluateDocumentSearchBehavior

    assert EvaluateDocumentSearchBehavior.__bases__ == (Transform,)

    plan = Compiler.frontend.compile()(EvaluateDocumentSearchBehavior, materialize_schemas=False).analysis
    assert isinstance(plan, TransformPlan)
    assert [output.name for output in plan.outputs] == [
        "request_behaviors",
        "daily_behavior",
    ]
    assert [step.name for step in plan.steps] == [
        "select_requests",
        "select_impressions",
        "attribute_clicks",
        "measure_impressions",
        "measure_requests",
        "calculate_reciprocal_rank",
        "publish_requests",
        "summarize_exposure",
        "summarize_requests",
        "publish_daily",
        "publish_request_behaviors",
        "publish_daily_behavior",
    ]


def test_experiment_evaluators_schedule_combined_selection_before_their_override() -> None:
    """Experiment evaluators reuse combined selection before adding experiment context."""

    from examples.search.transforms.experiments.evaluation.search_docs.eval_behavior import (
        EvaluateDocumentSearchBehavior,
    )
    from examples.search.transforms.experiments.evaluation.search_docs.eval_ranking import (
        EvaluateDocumentRankingQuality,
    )

    for evaluator, parent_step in (
        (EvaluateDocumentRankingQuality, "select_queries"),
        (EvaluateDocumentSearchBehavior, "select_requests"),
    ):
        plan = cast(TransformPlan, Compiler.frontend.compile()(evaluator, materialize_schemas=False).analysis)
        steps = plan.steps
        parent_origin = steps[0].origin
        child_origin = steps[1].origin

        assert [step.name for step in steps[:2]] == [
            f"EvaluateDocument{'RankingQuality' if parent_step == 'select_queries' else 'SearchBehavior'}.{parent_step}",
            parent_step,
        ]
        assert parent_origin is not None
        assert child_origin is not None
        assert parent_origin.module.startswith("examples.search.transforms.evaluation.search_docs")
        assert child_origin.module.startswith("examples.search.transforms.experiments")


def _paths_diff(actual: dict[str, str], expected: dict[str, str]) -> str:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    return f"missing generated files: {missing}\nextra generated files: {extra}"


def _text_diff(path: str, expected: str, actual: str) -> str:
    diff = difflib.unified_diff(
        expected.splitlines(),
        actual.splitlines(),
        fromfile=f"examples/{path}",
        tofile=f"actual/{path}",
        lineterm="",
    )
    return "\n".join(diff)
