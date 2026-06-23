from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.expr.Expression import Expression
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.Join import Join
from structure.app.dsl.logic.model.transforms.JoinHint import JoinHint


@dataclass(frozen=True)
class JoinPlan:
    input_name: str
    source: str
    input_schema: type[Structure]
    predicate: Expression
    how: Join
    hint: JoinHint | None = None
