from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.joins import TiePolicy


@dataclass(frozen=True)
class OrderedTimelineScanPlan:
    scope: str
    state_scope: str
    row_scope: str
    state_schema: type[Schema]
    initial: tuple[tuple[str, Expression], ...]
    transition: tuple[tuple[str, Expression], ...]
    partition_by: tuple[Expression, ...]
    order_by: tuple[Expression, ...]
    max_rows: int
    ties: TiePolicy
