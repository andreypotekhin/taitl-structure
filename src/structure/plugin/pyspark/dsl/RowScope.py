from __future__ import annotations

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


class RowScope:

    def __init__(
        self,
        *,
        name: str,
        schema: type[Schema],
        nullable: bool = False,
        nullable_reason: str | None = None,
    ) -> None:
        self._structure_scope_name = name
        self._structure_scope_schema = schema
        self._structure_scope_nullable = nullable
        self._structure_scope_nullable_reason = nullable_reason

    def __getattr__(self, name: str) -> Expression:
        fields = self._structure_scope_schema._structure_fields
        if name not in fields:
            raise AttributeError(name)
        field = fields[name]
        data = {
            "scope": self._structure_scope_name,
            "field": field.column,
            "name": field.name,
            "path": (field.column,),
            "name_path": (field.name,),
        }
        if self._structure_scope_nullable_reason is not None:
            data["nullable_reason"] = self._structure_scope_nullable_reason
        return Expression(
            kind="field",
            type=field.type,
            nullable=self._structure_scope_nullable or field.nullable,
            data=data,
        )

    def where(self, *predicates: object):
        from structure.plugin.pyspark.dsl.body import where

        return where(*predicates)

    def project(self, *args: object) -> object:
        from structure.plugin.pyspark.dsl.body import project

        return project(*args)
