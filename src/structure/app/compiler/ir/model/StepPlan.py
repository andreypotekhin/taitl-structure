from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.model.HookPlan import HookPlan
from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.compiler.ir.model.ProjectAssignment import ProjectAssignment
from structure.app.compiler.ir.model.StepInputPlan import StepInputPlan
from structure.app.compiler.ir.model.StepResultPlan import StepResultPlan
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.schemas.Structure import Structure


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
    operations: tuple[OperationPlan, ...] = ()
    before_hooks: tuple[HookPlan, ...] = ()
    after_hooks: tuple[HookPlan, ...] = ()
    inputs: tuple[StepInputPlan, ...] = ()
    results: tuple[StepResultPlan, ...] = ()
