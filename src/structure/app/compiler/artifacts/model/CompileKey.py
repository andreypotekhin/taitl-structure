from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CompileKey:
    subject: tuple[str, ...]
    structure_version: str
    options: tuple[object, ...]
    sources: tuple[tuple[str, int | None, int | None, str | None], ...]
