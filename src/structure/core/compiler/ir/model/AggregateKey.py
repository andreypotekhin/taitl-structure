from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.expr.Expression import Expression


@dataclass(frozen=True)
class AggregateKey:
    name: str
    expression: Expression
