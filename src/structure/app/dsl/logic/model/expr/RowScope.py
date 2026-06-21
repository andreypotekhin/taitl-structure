from __future__ import annotations

from structure.app.dsl.logic.model.expr.Expression import Expression
from structure.app.dsl.logic.model.schemas.Structure import Structure


class RowScope:

    def __init__(self, *, name: str, schema: type[Structure]) -> None:
        self._structure_scope_name = name
        self._structure_scope_schema = schema

    def __getattr__(self, name: str) -> Expression:
        fields = self._structure_scope_schema._structure_fields
        if name not in fields:
            raise AttributeError(name)
        field = fields[name]
        return Expression(
            kind="field",
            type=field.type,
            nullable=field.nullable,
            data={"scope": self._structure_scope_name, "field": field.name},
        )
