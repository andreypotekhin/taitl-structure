from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from structure.core.cli.api.CliApp import CliApp
from structure.core.configuration.model.StructureConfig import StructureConfig


def assert_check_success(
    *,
    project_root: Path | str | None = None,
    overrides: Mapping[str, object] | None = None,
    **settings: object,
) -> tuple[str, ...]:
    config = _config(project_root=project_root, overrides=overrides, settings=settings)
    lines = CliApp.check_project()(config)
    _assert_line(lines, "Structure check passed")
    return lines


def assert_compile_success(
    *,
    project_root: Path | str | None = None,
    overrides: Mapping[str, object] | None = None,
    **settings: object,
) -> tuple[str, ...]:
    config = _config(project_root=project_root, overrides=overrides, settings=settings)
    lines = CliApp.compile_project()(config)
    _assert_line(lines, "Structure compile passed")
    return lines


def assert_generated_fresh(
    *,
    project_root: Path | str | None = None,
    overrides: Mapping[str, object] | None = None,
    **settings: object,
) -> tuple[str, ...]:
    merged = dict(overrides or {})
    merged.update(settings)
    merged["fail_on_diff"] = True
    try:
        return assert_compile_success(project_root=project_root, overrides=merged)
    except Exception as error:
        raise AssertionError(
            "Generated Structure output is stale. "
            "Run `structure compile` and review the generated file diff.\n\n"
            f"{error}"
        ) from error


def assert_expected_diagnostic(
    action: Callable[[], Any],
    code: str,
    *,
    problem_contains: str | None = None,
    use_contains: str | None = None,
    source_endswith: str | None = None,
) -> Any:
    try:
        action()
    except Exception as error:
        diagnostic = getattr(error, "diagnostic", None)
        if diagnostic is None:
            raise AssertionError(f"Expected diagnostic {code}, got {type(error).__name__}: {error}") from error
        _assert_equal("diagnostic code", getattr(diagnostic, "code", None), code)
        if problem_contains is not None and problem_contains not in diagnostic.problem_text():
            raise AssertionError(
                f"Expected diagnostic {code} problem to contain {problem_contains!r}; "
                f"actual problem: {diagnostic.problem_text()!r}"
            )
        if use_contains is not None and use_contains not in diagnostic.use_text():
            raise AssertionError(
                f"Expected diagnostic {code} use guidance to contain {use_contains!r}; "
                f"actual guidance: {diagnostic.use_text()!r}"
            )
        if source_endswith is not None and not diagnostic.source.endswith(source_endswith):
            raise AssertionError(
                f"Expected diagnostic {code} source to end with {source_endswith!r}; "
                f"actual source: {diagnostic.source!r}"
            )
        return diagnostic
    raise AssertionError(f"Expected diagnostic {code}, but action completed successfully.")


def _config(
    *,
    project_root: Path | str | None,
    overrides: Mapping[str, object] | None,
    settings: Mapping[str, object],
) -> StructureConfig:
    merged = dict(overrides or {})
    duplicates = set(merged).intersection(settings)
    if duplicates:
        names = ", ".join(sorted(duplicates))
        raise ValueError(f"Configuration override supplied twice: {names}.")
    merged.update(settings)
    return StructureConfig.resolve(project_root=project_root, overrides=merged)


def _assert_line(lines: tuple[str, ...], expected: str) -> None:
    if expected not in lines:
        raise AssertionError(f"Expected command output line {expected!r}; actual lines: {lines!r}")


def _assert_equal(label: str, actual: object, expected: object) -> None:
    if actual != expected:
        raise AssertionError(f"Expected {label} {expected!r}; actual {actual!r}.")
