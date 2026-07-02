from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.schemas.FieldDefinition import FieldDefinition


@dataclass(frozen=True)
class AggregateAssignment:
    field: FieldDefinition
    function: str
    expression: Expression | None = None
    key: str | None = None
