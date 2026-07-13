from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.model.AggregatePlan import AggregatePlan
from structure.app.compiler.ir.model.HookPlan import HookPlan
from structure.app.compiler.ir.model.ProjectAssignment import ProjectAssignment
from structure.app.dsl.model.schemas.Schema import Schema


@dataclass(frozen=True)
class StepResultPlan:
    schema: type[Schema]
    lane: str
    frame: str
    projection: tuple[ProjectAssignment, ...]
    ordinal: int
    aggregate: AggregatePlan | None = None
    after_hooks: tuple[HookPlan, ...] = ()
