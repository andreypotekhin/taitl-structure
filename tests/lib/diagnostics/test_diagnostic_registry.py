import re
import sys
from pathlib import Path

import pytest

from structure.lib.cross.errors import (
    Diagnostic,
    DiagnosticEntry,
    DiagnosticRegistry,
    SourceExcerpt,
    SourceSpan,
    diagnostic_registry,
    render_diagnostic,
)

ROOT = Path(__file__).resolve().parents[3]


def test_diagnostic_registry_is_spark_free_and_valid() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    diagnostic_registry.validate()

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert diagnostic_registry["CONF-E0101"].title == "Unknown configuration key"


def test_diagnostic_registry_rejects_duplicates() -> None:
    entry = _entry("CONF-E0101")

    with pytest.raises(ValueError, match="Duplicate diagnostic code"):
        DiagnosticRegistry([entry, entry])


def test_diagnostic_registry_rejects_malformed_codes() -> None:
    with pytest.raises(ValueError, match="Malformed diagnostic code"):
        DiagnosticRegistry([_entry("CONF-0101")])


def test_diagnostic_registry_rejects_unknown_prefixes() -> None:
    with pytest.raises(ValueError, match="Unknown diagnostic prefix"):
        DiagnosticRegistry([_entry("NOPE-E0101")])


def test_diagnostic_registry_rejects_missing_published_docs() -> None:
    with pytest.raises(ValueError, match="missing docs link"):
        DiagnosticRegistry([_entry("CONF-E0101", docs="")])


def test_diagnostic_registry_rejects_deprecated_without_replacement() -> None:
    with pytest.raises(ValueError, match="missing replacement"):
        DiagnosticRegistry([_entry("CONF-E0101", status="deprecated")])


def test_public_docs_contain_anchors_for_active_registry_entries() -> None:
    text = (ROOT / "docs" / "Diagnostics.md").read_text(encoding="utf-8").lower()
    anchors = {match.group(1).strip().lower() for match in re.finditer(r"^###\s+(.+)$", text, re.MULTILINE)}

    missing = []
    for entry in diagnostic_registry.entries():
        if entry.status == "active" and entry.docs.startswith("docs/Diagnostics.md#"):
            anchor = entry.docs.rsplit("#", 1)[1]
            if anchor not in anchors:
                missing.append(entry.code)

    assert missing == []


def test_diagnostic_value_uses_registry_defaults() -> None:
    diagnostic = Diagnostic(entry=diagnostic_registry["GEN-E0901"], context={"generated_dir": "generated"})

    assert diagnostic.code == "GEN-E0901"
    assert diagnostic.severity == "error"
    assert diagnostic.title == "Generated output is stale"
    assert "Generated files differ" in diagnostic.problem_text()
    assert "structure compile" in diagnostic.use_text()


def test_renderer_annotates_a_source_span_without_changing_named_sections() -> None:
    span = SourceSpan(
        path="orders/transforms/normalize.py",
        start_line=24,
        start_column=11,
        end_line=24,
        end_column=32,
        label="may produce null",
        excerpt=SourceExcerpt(first_line=23, lines=("    return Target(", "        total=order.discounted_total(),")),
    )
    related = SourceSpan(
        path="orders/schemas/target.py",
        start_line=8,
        start_column=5,
        end_line=8,
        end_column=10,
        label="declared non-nullable here",
        excerpt=SourceExcerpt(first_line=8, lines=("    total = field.decimal(nullable=False)",)),
    )

    rendered = render_diagnostic(
        Diagnostic(entry=diagnostic_registry["SCHEMA-E0301"], primary_span=span, related_spans=(related,)),
        kind="CompileError",
    )

    assert "Source:\n  --> orders/transforms/normalize.py:24:11" in rendered
    assert "total=order.discounted_total()," in rendered
    assert "^^^^^^^^^^^^^^^^^^^^^ may produce null" in rendered
    assert "::: orders/schemas/target.py:8:5" in rendered
    assert "Problem:" in rendered
    assert "Use:" in rendered


def test_renderer_keeps_the_logical_source_when_no_span_is_available() -> None:
    rendered = render_diagnostic(
        Diagnostic(entry=diagnostic_registry["DSL-E0402"], source="orders.Normalize.normalize"), kind="CompileError"
    )

    assert "Source:\n  orders.Normalize.normalize" in rendered
    assert "-->" not in rendered


@pytest.mark.parametrize(
    "span",
    [
        SourceSpan("orders.py", 1, 1, 1, 2),
        SourceSpan("orders.py", 1, 1, 2, 1),
    ],
)
def test_source_span_accepts_valid_coordinates(span: SourceSpan) -> None:
    assert span.path == "orders.py"


@pytest.mark.parametrize(
    "args",
    [
        ("/private/orders.py", 1, 1, 1, 2),
        ("C:\\orders.py", 1, 1, 1, 2),
        ("../orders.py", 1, 1, 1, 2),
        ("orders.py", 0, 1, 1, 2),
        ("orders.py", 1, 2, 1, 2),
    ],
)
def test_source_span_rejects_invalid_coordinates_and_paths(args: tuple[str, int, int, int, int]) -> None:
    with pytest.raises(ValueError):
        SourceSpan(*args)


def _entry(code: str, *, docs: str = "docs/Diagnostics.md#conf-e0101", status: str = "active") -> DiagnosticEntry:
    return DiagnosticEntry(
        code=code,
        severity="error",
        title="Test diagnostic",
        owner="test",
        status=status,
        docs=docs,
        introduced="1.0.0",
        problem_template="Problem.",
        use_template="Use.",
    )
