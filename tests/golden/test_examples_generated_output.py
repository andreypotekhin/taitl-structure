from __future__ import annotations

import difflib

import pytest
from helpers.example_projects import (
    expected_orders_generated,
    expected_stocks_generated,
    expected_streams_generated,
    expected_texts_generated,
    render_orders_example,
    render_stocks_example,
    render_streams_example,
    render_texts_example,
)


@pytest.mark.parametrize(
    ("actual", "expected"),
    [
        (render_orders_example, expected_orders_generated),
        (render_streams_example, expected_streams_generated),
        (render_stocks_example, expected_stocks_generated),
        (render_texts_example, expected_texts_generated),
    ],
)
def test_example_generated_output_matches_golden_files(actual, expected) -> None:
    actual = actual()
    expected = expected()

    assert set(actual) == set(expected), _paths_diff(actual, expected)
    for path in expected:
        assert actual[path] == expected[path], _text_diff(path, expected[path], actual[path])


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
