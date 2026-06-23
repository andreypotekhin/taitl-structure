from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.logic.model.HookPlan import HookPlan
from structure.app.compiler.ir.logic.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.logic.model.ProjectAssignment import ProjectAssignment
from structure.app.compiler.ir.logic.model.StepInputPlan import StepInputPlan
from structure.app.compiler.ir.logic.model.StepResultPlan import StepResultPlan
from structure.app.dsl.logic.model.expr.Expression import Expression
from structure.app.dsl.logic.model.schemas.Structure import Structure


@dataclass(frozen=True)
class StepPlan:
    name: str
    input_schema: type[Structure]
    output_schema: type[Structure]
    source: str
    source_scope: str
    input_lane: str
    output_lane: str
    filters: tuple[Expression, ...]
    projection: tuple[ProjectAssignment, ...]
    ordinal: int
    joins: tuple[JoinPlan, ...] = ()
    before_hooks: tuple[HookPlan, ...] = ()
    after_hooks: tuple[HookPlan, ...] = ()
    inputs: tuple[StepInputPlan, ...] = ()
    results: tuple[StepResultPlan, ...] = ()
