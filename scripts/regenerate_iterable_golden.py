"""Regenerate the checked-in Iterable school generated-code fixture."""

from pathlib import Path

from helpers.example_projects import render_school_iterable_example


def main() -> None:
    root = Path(__file__).parents[1]
    for relative, text in render_school_iterable_example().items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
