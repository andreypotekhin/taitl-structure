from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.expr.Expression import Expression
from structure.app.dsl.logic.model.plans.ProjectAssignment import ProjectAssignment
from structure.app.dsl.logic.model.schemas.Structure import Structure


@dataclass(frozen=True)
class StepPlan:
    name: str
    input_schema: type[Structure]
    output_schema: type[Structure]
    filters: tuple[Expression, ...]
    projection: tuple[ProjectAssignment, ...]
    ordinal: int
