from __future__ import annotations

from collections.abc import Callable
from types import FunctionType

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

    def __getattr__(self, name: str) -> Expression | Callable[..., object]:
        fields = self._structure_scope_schema._structure_fields
        if name in fields:
            return self._field(name, fields[name])
        method = self._schema_method(name)
        if method is not None:
            return method.__get__(self, type(self))
        raise AttributeError(name)

    def _field(self, name: str, field) -> Expression:
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

    def _schema_method(self, name: str) -> FunctionType | None:
        for schema in self._structure_scope_schema.__mro__:
            if schema is Schema:
                break
            method = schema.__dict__.get(name)
            if isinstance(method, FunctionType):
                return method
        return None

    def where(self, *predicates: object):
        from structure.plugin.pyspark.dsl.body import where

        return where(*predicates)

    def project(self, *args: object) -> object:
        from structure.plugin.pyspark.dsl.body import project

        return project(*args)
