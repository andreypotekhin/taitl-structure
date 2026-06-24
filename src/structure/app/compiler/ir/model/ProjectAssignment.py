from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.schemas.FieldDefinition import FieldDefinition


@dataclass(frozen=True)
class ProjectAssignment:
    field: FieldDefinition
    expression: Expression
