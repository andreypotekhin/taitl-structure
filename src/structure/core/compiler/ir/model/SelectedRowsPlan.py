from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.expr.Expression import Expression
from structure.core.dsl.model.transforms.TiePolicy import TiePolicy


@dataclass(frozen=True)
class SelectedRowsPlan:
    direction: str
    order_by: Expression
    partition_by: tuple[Expression, ...]
    ties: TiePolicy
