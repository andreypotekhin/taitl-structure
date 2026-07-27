from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.joins import TiePolicy


@dataclass(frozen=True)
class RelationPrioritySelectionPlan:
    keys: tuple[Expression, ...]
    predicate: Expression
    order_by: Expression
    missing: str
    ties: TiePolicy
