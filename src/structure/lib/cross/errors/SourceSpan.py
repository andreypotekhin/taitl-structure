from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePath, PureWindowsPath


@dataclass(frozen=True)
class SourceExcerpt:
    first_line: int
    lines: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.first_line < 1:
            raise ValueError("Source excerpt line numbers start at 1.")
        if not self.lines:
            raise ValueError("Source excerpt must contain at least one line.")


@dataclass(frozen=True)
class SourceSpan:
    path: str
    start_line: int
    start_column: int
    end_line: int
    end_column: int
    label: str = ""
    excerpt: SourceExcerpt | None = None

    def __post_init__(self) -> None:
        path = PurePath(self.path)
        if not self.path or path.is_absolute() or PureWindowsPath(self.path).is_absolute() or ".." in path.parts:
            raise ValueError("Source span path must be a nonempty relative display path.")
        if min(self.start_line, self.start_column, self.end_line, self.end_column) < 1:
            raise ValueError("Source span lines and columns start at 1.")
        if (self.end_line, self.end_column) <= (self.start_line, self.start_column):
            raise ValueError("Source span ends before it starts.")
        if self.excerpt and not self.excerpt.first_line <= self.start_line < self.excerpt.first_line + len(
            self.excerpt.lines
        ):
            raise ValueError("Source excerpt must contain the source span start line.")
