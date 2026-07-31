from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema


@dataclass(frozen=True)
class RelationSetPlan:
    operation: str
    input_name: str
    source: str
    schema: type[Schema]
    by_name: bool
    allow_missing_columns: bool = False
