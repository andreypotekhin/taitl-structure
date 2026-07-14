from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_execution_docs_are_canonical_and_online_docs_redirect() -> None:
    """Public execution links use the canonical term while old links remain discoverable."""

    background = ROOT / "docs" / "background"
    specification = ROOT / "docs" / "dev" / "specifications"

    assert "# Execution" in (background / "Execution.back.md").read_text(encoding="utf-8")
    assert "[Execution](Execution.back.md)" in (background / "OnlineExecution.back.md").read_text(encoding="utf-8")
    assert "# Execution" in (specification / "Execution.md").read_text(encoding="utf-8")
    assert "[Execution](Execution.md)" in (specification / "OnlineExecution.md").read_text(encoding="utf-8")


def test_public_docs_link_execution_without_the_legacy_filename() -> None:
    """Public references lead to the canonical execution document."""

    paths = [ROOT / "docs" / "QuickRef.md", ROOT / "docs" / "reference" / "API.ref.md"]
    text = "\n".join(path.read_text(encoding="utf-8") for path in paths)

    assert "Execution.back.md" in text
    assert "OnlineExecution.back.md" not in text
