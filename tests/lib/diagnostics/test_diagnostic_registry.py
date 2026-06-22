import re
import sys
from pathlib import Path

import pytest

from structure.lib.cross.errors import Diagnostic, DiagnosticEntry, DiagnosticRegistry, diagnostic_registry


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
    text = Path("docs/Diagnostics.md").read_text(encoding="utf-8").lower()
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
