from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema


@dataclass(frozen=True)
class PySparkRelationSetRecipe:
    operation: str
    input_name: str
    source: str
    schema: type[Schema]
    by_name: bool
