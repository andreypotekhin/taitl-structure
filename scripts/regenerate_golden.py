"""Regenerate checked-in generated-output fixtures for every example project."""

from collections.abc import Callable
from pathlib import Path

from helpers.example_projects import (
    render_school_iterable_example,
    render_search_example,
    render_security_example,
    render_stocks_example,
    render_store_example,
    render_streams_example,
)


ROOT = Path(__file__).parents[1]
Render = Callable[[], dict[str, str]]
RENDERERS: tuple[Render, ...] = (
    render_store_example,
    render_streams_example,
    render_stocks_example,
    render_security_example,
    render_search_example,
    render_school_iterable_example,
)


def main() -> None:
    files = {path: text for render in RENDERERS for path, text in render().items()}
    _remove_stale_files(files)
    for relative, text in files.items():
        path = ROOT / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def _remove_stale_files(files: dict[str, str]) -> None:
    generated_roots = {ROOT / Path(*Path(path).parts[:3]) for path in files}
    expected = {ROOT / path for path in files}
    for generated_root in generated_roots:
        for path in generated_root.rglob("*"):
            if path.is_file() and path not in expected:
                path.unlink()


if __name__ == "__main__":
    main()
