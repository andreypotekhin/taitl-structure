from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.Join import Join
from structure.app.dsl.model.transforms.JoinHint import JoinHint


@dataclass(frozen=True)
class JoinPlan:
    input_name: str
    source: str
    input_schema: type[Structure]
    predicate: Expression
    how: Join
    hint: JoinHint | None = None
    method: JoinMethod = JoinMethod.ONE
