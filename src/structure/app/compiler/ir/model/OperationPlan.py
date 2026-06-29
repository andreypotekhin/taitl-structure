from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.dsl.model.expr.Expression import Expression


@dataclass(frozen=True)
class OperationPlan:
    kind: str
    filter: Expression | None = None
    join: JoinPlan | None = None

    @staticmethod
    def filter_operation(predicate: Expression) -> "OperationPlan":
        return OperationPlan(kind="filter", filter=predicate)

    @staticmethod
    def join_operation(join: JoinPlan) -> "OperationPlan":
        return OperationPlan(kind="join", join=join)
