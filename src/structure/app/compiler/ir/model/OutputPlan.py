from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.model.ProjectAssignment import ProjectAssignment
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.schemas.Structure import Structure


@dataclass(frozen=True)
class OutputPlan:
    name: str
    schema: type[Structure]
    source: str
    source_scope: str
    source_schema: type[Structure]
    filters: tuple[Expression, ...]
    projection: tuple[ProjectAssignment, ...]
    ordinal: int
    joins: tuple[JoinPlan, ...] = ()
