from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema


@dataclass(frozen=True)
class PySparkSqlRecipe:
    statement: str
    relations: tuple[tuple[str, str], ...]
    args: object | None
    schema: type[Schema]
    label: str | None
    scope: str
    step: str
