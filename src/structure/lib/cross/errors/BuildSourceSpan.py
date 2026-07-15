from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

from structure.lib.cross.errors.SourceSpan import SourceExcerpt, SourceSpan


class BuildSourceSpan:

    def __call__(self, value: Any, *, project_root: Path | None = None, label: str = "") -> SourceSpan | None:
        try:
            path = inspect.getsourcefile(value)
            lines, first_line = inspect.getsourcelines(value)
        except (OSError, TypeError):
            return None
        if path is None or not lines:
            return None
        source = Path(path)
        display = self._display(source, value, project_root)
        line_offset = self._declaration_offset(lines)
        line = lines[line_offset].rstrip("\n")
        start_line = first_line + line_offset
        excerpt_start = max(0, line_offset - 2)
        excerpt = tuple(item.rstrip("\n") for item in lines[excerpt_start : excerpt_start + 5])
        return SourceSpan(
            path=display,
            start_line=start_line,
            start_column=1,
            end_line=start_line,
            end_column=max(2, len(line) + 1),
            label=label,
            excerpt=SourceExcerpt(first_line=first_line + excerpt_start, lines=excerpt),
        )

    def _display(self, path: Path, value: object, project_root: Path | None) -> str:
        roots = (project_root, Path.cwd()) if project_root is not None else (Path.cwd(),)
        for root in roots:
            try:
                return path.relative_to(root).as_posix()
            except ValueError:
                continue
        module = getattr(value, "__module__", "source")
        name = getattr(value, "__qualname__", getattr(value, "__name__", "source"))
        return f"{module}.{name}"

    def _declaration_offset(self, lines: list[str]) -> int:
        for index, line in enumerate(lines):
            stripped = line.lstrip()
            if stripped.startswith(("def ", "async def ", "class ")):
                return index
        return 0


build_source_span = BuildSourceSpan()
