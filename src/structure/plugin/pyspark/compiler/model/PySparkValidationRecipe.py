from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema, SchemaMode


@dataclass(frozen=True)
class PySparkValidationRecipe:
    target: str
    schema: type[Schema]
    mode: SchemaMode
    project: bool
    reason: str
    check: bool = True
    boundary: bool = False
