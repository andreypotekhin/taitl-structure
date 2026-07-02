from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.transforms.TiePolicy import TiePolicy


@dataclass(frozen=True)
class JoinDedupe:
    order_by: Expression
    direction: str
    ties: TiePolicy = TiePolicy.ERROR

    @staticmethod
    def latest_by(order_by: object, *, ties: TiePolicy = TiePolicy.ERROR) -> "JoinDedupe":
        return JoinDedupe._policy(order_by, direction="latest", ties=ties)

    @staticmethod
    def earliest_by(order_by: object, *, ties: TiePolicy = TiePolicy.ERROR) -> "JoinDedupe":
        return JoinDedupe._policy(order_by, direction="earliest", ties=ties)

    @staticmethod
    def _policy(order_by: object, *, direction: str, ties: TiePolicy) -> "JoinDedupe":
        if not isinstance(order_by, Expression):
            raise TypeError(f"JoinDedupe.{direction}_by(order_by=...) requires a Structure expression")
        return JoinDedupe(order_by=cast(Expression, order_by), direction=direction, ties=ties)
