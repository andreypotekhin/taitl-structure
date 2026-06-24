from __future__ import annotations

from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.schemas.Structure import Structure


class RowScope:

    def __init__(self, *, name: str, schema: type[Structure], nullable: bool = False) -> None:
        self._structure_scope_name = name
        self._structure_scope_schema = schema
        self._structure_scope_nullable = nullable

    def __getattr__(self, name: str) -> Expression:
        fields = self._structure_scope_schema._structure_fields
        if name not in fields:
            raise AttributeError(name)
        field = fields[name]
        return Expression(
            kind="field",
            type=field.type,
            nullable=self._structure_scope_nullable or field.nullable,
            data={"scope": self._structure_scope_name, "field": field.name},
        )
