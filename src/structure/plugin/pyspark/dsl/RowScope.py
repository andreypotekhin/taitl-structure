"""Row-level field access for PySpark DSL authoring."""

from __future__ import annotations

from collections.abc import Callable
from types import FunctionType

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


class RowScope:
    """A symbolic view of one row in a declared Structure schema.

    Attribute access resolves declared schema fields into typed
    :class:`Expression` objects.  Methods declared directly on the schema class
    are also rebound to the scope so reusable row-level calculations can be
    authored once and used from compiled PySpark steps.
    """

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
        """Continue a chained body declaration with ``where(...)``."""
        from structure.plugin.pyspark.dsl.body import where

        return where(*predicates)

    def project(self, *args: object) -> object:
        """Continue a chained body declaration with ``project(...)``."""
        from structure.plugin.pyspark.dsl.body import project

        return project(*args)

    def persist(self, storage_level: object | None = None):
        from structure.plugin.pyspark.dsl.operations_api import persist

        return persist(storage_level)

    def cache(self):
        from structure.plugin.pyspark.dsl.operations_api import cache

        return cache()

    def unpersist(self, *, blocking: bool = False):
        from structure.plugin.pyspark.dsl.operations_api import unpersist

        return unpersist(blocking=blocking)

    def checkpoint(self, *, eager: bool = True):
        from structure.plugin.pyspark.dsl.operations_api import checkpoint

        return checkpoint(eager=eager)

    def local_checkpoint(self, *, eager: bool = True):
        from structure.plugin.pyspark.dsl.operations_api import local_checkpoint

        return local_checkpoint(eager=eager)
