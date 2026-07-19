from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure.platform.pyspark.dsl.aggregation.AggregateAssignment import AggregateAssignment
from structure.platform.pyspark.dsl.aggregation.AggregateKey import AggregateKey


@dataclass(frozen=True)
class AggregatePlan:
    keys: tuple[AggregateKey, ...]
    assignments: tuple[AggregateAssignment, ...]
    grouping: str = "group_by"
    levels: tuple[tuple[str, ...], ...] = ()
    having: Any | None = None
