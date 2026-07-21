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
