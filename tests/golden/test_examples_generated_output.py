from __future__ import annotations

import difflib

import pytest
from helpers.example_projects import (
    ROOT,
    expected_search_generated,
    expected_security_generated,
    expected_stocks_generated,
    expected_store_generated,
    expected_streams_generated,
    render_search_example,
    render_security_example,
    render_stocks_example,
    render_store_example,
    render_streams_example,
)

from structure import StructureConfig
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
    score_all = next(
        transform
        for transform in project.transforms
        if transform.__module__ == "examples.search.transforms.scoring.ScoreAll" and transform.__name__ == "ScoreAll"
    )

    Compiler.frontend.compile()(score_all, config=config, materialize_schemas=False)


def test_similarity_public_transform_inherits_its_searching_implementation() -> None:
    """The public transform stays import-stable while its implementation is grouped with search transforms."""

    from examples.search.transforms.searching.search_similarity import Similarity as SearchSimilarity
    from examples.search.transforms.similarity import Similarity

    assert Similarity.__bases__ == (SearchSimilarity,)


def test_behavior_evaluator_inherits_ordered_partial_stages() -> None:
    """The public evaluator publishes its inherited request-to-daily behavior pipeline."""

    from examples.search.transforms.evaluation.search_docs.behavior.EvaluateDocumentSearchBehavior import (
        EvaluateDocumentSearchBehavior,
        MeasureDocumentSearchImpressions,
        MeasureDocumentSearchRequests,
        SelectDocumentSearchRequests,
        SummarizeDocumentSearchBehavior,
    )

    assert MeasureDocumentSearchImpressions.__bases__ == (SelectDocumentSearchRequests,)
    assert MeasureDocumentSearchRequests.__bases__ == (MeasureDocumentSearchImpressions,)
    assert SummarizeDocumentSearchBehavior.__bases__ == (MeasureDocumentSearchRequests,)
    assert EvaluateDocumentSearchBehavior.__bases__ == (SummarizeDocumentSearchBehavior,)

    stages = (
        (SelectDocumentSearchRequests, ["selected"]),
        (MeasureDocumentSearchImpressions, ["selected", "measured"]),
        (MeasureDocumentSearchRequests, ["selected", "measured", "measured_requests"]),
        (
            SummarizeDocumentSearchBehavior,
            ["selected", "measured", "measured_requests", "summarized_daily"],
        ),
    )
    for stage, outputs in stages:
        plan = Compiler.frontend.compile()(stage, materialize_schemas=False).analysis
        assert isinstance(plan, TransformPlan)
        assert [output.name for output in plan.outputs] == outputs

    plan = Compiler.frontend.compile()(EvaluateDocumentSearchBehavior, materialize_schemas=False).analysis
    assert isinstance(plan, TransformPlan)
    assert [output.name for output in plan.outputs] == [
        "selected",
        "measured",
        "measured_requests",
        "summarized_daily",
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
