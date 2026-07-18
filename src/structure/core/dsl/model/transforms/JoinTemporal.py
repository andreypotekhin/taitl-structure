from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.expr.Expression import Expression
from structure.core.dsl.model.transforms.OverlapPolicy import OverlapPolicy


@dataclass(frozen=True)
class JoinTemporal:
    at: Expression
    valid_from: Expression
    valid_to: Expression
    overlaps: OverlapPolicy = OverlapPolicy.ERROR
