from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class RelationHierarchyFallbackPlan:
    source_id: Expression
    path: Expression
    parent_input: str
    parent_source: str
    parent_schema: type[Schema]
    parent_id: Expression
    parent: Expression
    schema: type[Schema]
    scope: str
    source: str
    fallback: str
    ordinal: str
    separator: str
    max_depth: int
