import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MATRIX = ROOT / "docs/dev/specifications/PySparkApiCatalog.md"


def test_v6_executable_specification_matrix_references_real_tests() -> None:
    text = MATRIX.read_text(encoding="utf-8")
    tests = sorted(set(re.findall(r"`(tests/[^`]+?\.py)`", text)))

    assert len(tests) >= 10
    assert all((ROOT / test).is_file() for test in tests)


def test_v6_executable_specification_matrix_names_required_evidence_topics() -> None:
    text = MATRIX.read_text(encoding="utf-8")

    for topic in (
        "source capture",
        "generated rendering",
        "online execution",
        "traceability",
        "compatibility",
        "live behavior",
    ):
        assert topic in text
