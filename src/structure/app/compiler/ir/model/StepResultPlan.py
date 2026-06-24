from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.model.HookPlan import HookPlan
from structure.app.compiler.ir.model.ProjectAssignment import ProjectAssignment
from structure.app.dsl.model.schemas.Structure import Structure


@dataclass(frozen=True)
class StepResultPlan:
    schema: type[Structure]
    lane: str
    frame: str
    projection: tuple[ProjectAssignment, ...]
    ordinal: int
    after_hooks: tuple[HookPlan, ...] = ()
