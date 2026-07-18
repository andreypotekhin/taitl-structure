from __future__ import annotations

from dataclasses import dataclass

from structure.core.compiler.ir.model.JoinPlan import JoinPlan
from structure.core.compiler.ir.model.OperationPlan import OperationPlan
from structure.core.compiler.ir.model.ProjectAssignment import ProjectAssignment
from structure.core.dsl.model.expr.Expression import Expression
from structure.core.dsl.model.schemas.Schema import Schema


@dataclass(frozen=True)
class OutputPlan:
    name: str
    schema: type[Schema]
    source: str
    source_scope: str
    source_schema: type[Schema]
    filters: tuple[Expression, ...]
    projection: tuple[ProjectAssignment, ...]
    ordinal: int
    joins: tuple[JoinPlan, ...] = ()
    operations: tuple[OperationPlan, ...] = ()
    aliases: tuple[str, ...] = ()
