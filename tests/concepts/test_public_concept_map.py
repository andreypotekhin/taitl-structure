from __future__ import annotations

from pathlib import Path


def test_public_concept_map_names_starting_concepts_and_test_owners() -> None:
    text = Path("tests/concepts/README.md").read_text(encoding="utf-8")

    for concept in (
        "configuration",
        "schema",
        "transform invocation",
        "expressions and filtering",
        "projection",
        "joins",
        "hooks",
        "generated PySpark",
        "online execution",
        "diagnostics",
        "compatibility",
    ):
        assert concept in text

    assert "docs/dev/Concepts.md" in text
    assert "tests/golden" in text
    assert "tests/specifications/compatibility" in text
