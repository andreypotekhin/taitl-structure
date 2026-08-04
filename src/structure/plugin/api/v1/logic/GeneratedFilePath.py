from __future__ import annotations

from pathlib import Path


def generated_file_target(path: str, *, root: Path) -> Path:
    """Resolve a generated file beneath root without permitting path escape."""

    if not isinstance(path, str) or not path or "\\" in path:
        raise ValueError(f"Generated file path must be a non-empty relative POSIX path: {path!r}")
    parts = path.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Generated file path must be a normalized relative path: {path!r}")

    root_path = root.resolve()
    target = (root_path / Path(*parts)).resolve()
    try:
        target.relative_to(root_path)
    except ValueError as error:
        raise ValueError(f"Generated file path escapes its output root: {path!r}") from error
    return target
