from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.logic.model.HookPlan import HookPlan
from structure.app.compiler.ir.logic.model.ProjectAssignment import ProjectAssignment
from structure.app.dsl.logic.model.schemas.Structure import Structure


@dataclass(frozen=True)
class StepResultPlan:
    schema: type[Structure]
    lane: str
    frame: str
    projection: tuple[ProjectAssignment, ...]
    ordinal: int
    after_hooks: tuple[HookPlan, ...] = ()
