from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.transforms.AsOf import AsOf
from structure.app.dsl.model.transforms.TiePolicy import TiePolicy


@dataclass(frozen=True)
class JoinAsOf:
    left_time: Expression
    right_time: Expression
    direction: AsOf = AsOf.BACKWARD
    tolerance: Expression | None = None
    ties: TiePolicy = TiePolicy.ERROR
