from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class RelationOrderPlan:
    order_by: tuple[Expression, ...]
