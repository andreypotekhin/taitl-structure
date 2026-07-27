from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class RelationHierarchyClosurePlan:
    id: Expression
    parent: Expression
    schema: type[Schema]
    scope: str
    node: str
    ancestor: str
    depth: str
    max_depth: int
