from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.model.AggregateAssignment import AggregateAssignment
from structure.app.compiler.ir.model.AggregateKey import AggregateKey


@dataclass(frozen=True)
class AggregatePlan:
    keys: tuple[AggregateKey, ...]
    assignments: tuple[AggregateAssignment, ...]
    grouping: str = "group_by"
