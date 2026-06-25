from pathlib import Path

import pytest

from structure.lib.cross.errors import Diagnostic, DiagnosticEntry, DiagnosticRegistry, diagnostic_registry


def test_diagnostic_registry_exposes_stable_codes_and_docs_links() -> None:
    """I can rely on stable diagnostic codes."""

    diagnostic_registry.validate()

    assert diagnostic_registry["DSL-E0401"].docs == "docs/Diagnostics.md#dsl-e0401"
    assert diagnostic_registry["SCHEMA-E0301"].severity == "error"
    assert diagnostic_registry["JOIN-W0601"].severity == "warning"
    assert diagnostic_registry["CLI-X1101"].severity == "internal"


def test_public_diagnostics_documentation_contains_active_anchors() -> None:
    """I can look up a diagnostic code in public documentation."""

    text = Path("docs/Diagnostics.md").read_text(encoding="utf-8").lower()

    for code in ["dsl-e0401", "schema-e0301", "join-w0601", "online-e1201", "stream-w0801"]:
        assert f"### {code}" in text


def test_diagnostic_values_render_problem_and_use_guidance_from_registry() -> None:
    """Diagnostic values render severities and remedy text consistently."""

    diagnostic = Diagnostic(entry=diagnostic_registry["GEN-E0901"], context={"generated_dir": "generated"})

    assert diagnostic.code == "GEN-E0901"
    assert diagnostic.severity == "error"
    assert "Generated files differ" in diagnostic.problem_text()
    assert "structure compile" in diagnostic.use_text()


def test_diagnostic_registry_rejects_duplicate_codes() -> None:
    """Duplicate diagnostic codes are rejected during registry validation."""

    entry = DiagnosticEntry(
        code="CONF-E0101",
        severity="error",
        title="Test diagnostic",
        owner="test",
        status="active",
        docs="docs/Diagnostics.md#conf-e0101",
        introduced="1.0.0",
        problem_template="Problem.",
        use_template="Use.",
    )

    with pytest.raises(ValueError, match="Duplicate diagnostic code"):
        DiagnosticRegistry([entry, entry])
