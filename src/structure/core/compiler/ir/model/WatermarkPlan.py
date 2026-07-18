from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.expr.Expression import Expression


@dataclass(frozen=True)
class WatermarkPlan:
    expression: Expression
    delay: str

    @property
    def scope(self) -> str:
        data = self.expression.data or {}
        return str(data.get("scope", ""))

    @property
    def column(self) -> str:
        data = self.expression.data or {}
        return str(data.get("field", ""))
