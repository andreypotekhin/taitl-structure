from __future__ import annotations

import difflib
from collections.abc import Mapping
from pathlib import Path


def generated_files(root: Path | str) -> dict[str, str]:
    path = Path(root)
    if not path.exists():
        return {}
    return {
        target.relative_to(path).as_posix(): target.read_text(encoding="utf-8")
        for target in sorted(path.rglob("*"))
        if target.is_file() and "__pycache__" not in target.parts and target.suffix != ".pyc"
    }


def assert_generated_snapshot(
    actual: Mapping[str, str] | Path | str,
    expected: Mapping[str, str] | Path | str,
    *,
    actual_label: str = "actual",
    expected_label: str = "expected",
) -> None:
    actual_files = _files(actual)
    expected_files = _files(expected)
    if set(actual_files) != set(expected_files):
        raise AssertionError(_paths_diff(actual_files, expected_files))
    for path in expected_files:
        if actual_files[path] != expected_files[path]:
            raise AssertionError(
                _text_diff(path, expected_files[path], actual_files[path], actual_label, expected_label)
            )


def _files(source: Mapping[str, str] | Path | str) -> dict[str, str]:
    if isinstance(source, Mapping):
        return {Path(path).as_posix(): text for path, text in source.items()}
    return generated_files(source)


def _paths_diff(actual: Mapping[str, str], expected: Mapping[str, str]) -> str:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    return f"missing generated files: {missing}\nextra generated files: {extra}"


def _text_diff(path: str, expected: str, actual: str, actual_label: str, expected_label: str) -> str:
    diff = difflib.unified_diff(
        expected.splitlines(),
        actual.splitlines(),
        fromfile=f"{expected_label}/{path}",
        tofile=f"{actual_label}/{path}",
        lineterm="",
    )
    return "\n".join(diff)
