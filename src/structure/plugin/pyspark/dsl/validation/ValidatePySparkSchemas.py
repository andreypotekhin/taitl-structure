"""PySpark schema validation for Structure ``Schema`` declarations."""

from __future__ import annotations

import builtins
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from types import UnionType
from typing import Union, get_args, get_origin, get_type_hints

from structure.dsl import FieldDefinition, Schema
from structure.plugin.pyspark.dsl.types import (
    Array,
    ArrayType,
    Binary,
    BinaryType,
    Boolean,
    BooleanType,
    Date,
    DateType,
    DecimalType,
    Double,
    DoubleType,
    Float,
    FloatType,
    Integer,
    IntegerType,
    Long,
    LongType,
    Map,
    MapType,
    String,
    StringType,
    Struct,
    StructType,
    StructureType,
    Timestamp,
    TimestampType,
)


class ValidatePySparkSchemas:
    """Resolve and validate field declarations against Spark type semantics.

    The validator keeps PySpark schema declarations strict: duplicate Spark
    aliases fail early, Python annotations must agree with field factories, and
    nested struct composition must remain acyclic so Spark schema materialization
    is deterministic.
    """

    def __call__(self, schema: type[Schema]) -> None:
        """Validate all declared fields on ``schema``."""
        self.validate(schema, schema._structure_fields)

    def validate(self, schema: type[Schema], fields: Mapping[str, FieldDefinition]) -> None:
        """Resolve annotations and validate a field mapping for ``schema``."""
        self._resolve(schema, fields)
        columns: dict[str, str] = {}
        for field in fields.values():
            other = columns.get(field.column)
            if other is not None:
                raise ValueError(f"{schema.__name__} has duplicate Spark column name {field.column!r}. Use a unique field alias.")
            columns[field.column] = field.name
            self._acyclic(schema, field.type, (schema,))

    def _resolve(
        self, schema: type[Schema], fields: Mapping[str, FieldDefinition], resolving: set[type[Schema]] | None = None
    ) -> None:
        resolving = resolving or set()
        if schema in resolving:
            return
        resolving.add(schema)
        hints = self._hints(schema)
        resolved = {
            name: self._field(schema, field, hints.get(name, field.hint))
            for name, field in fields.items()
        }
        if isinstance(fields, dict):
            fields.update(resolved)
        if fields is getattr(schema, "_structure_fields", None):
            schema._structure_local_fields = {
                name: resolved[name]
                for name in schema._structure_local_fields
                if name in resolved
            }

        for field in resolved.values():
            self._resolve_nested(field.type, resolving)
        resolving.remove(schema)

    @staticmethod
    def _hints(schema: type[Schema]) -> Mapping[str, object]:
        try:
            return get_type_hints(schema)
        except (NameError, TypeError) as error:
            raise TypeError(f"{schema.__name__} has an unresolved schema field annotation: {error}") from error

    def _field(self, schema: type[Schema], field: FieldDefinition, hint: object | None) -> FieldDefinition:
        hint = self._normalize_hint(hint)
        if hint is None:
            if not isinstance(field.type, StructureType):
                raise TypeError(f"{schema.__name__}.{field.name} needs a PySpark field factory or a supported Python hint.")
            return field

        if field.type.name == "__annotation__":
            return replace(field, type=self._infer(schema, field.name, hint))
        if not isinstance(field.type, StructureType):
            raise TypeError(f"{schema.__name__}.{field.name} uses an invalid PySpark field declaration.")
        if not self._compatible(hint, field.type):
            raise TypeError(
                f"{schema.__name__}.{field.name} hint {self._hint_text(hint)} is incompatible with "
                f"{self._type_text(field.type)}. Use a compatible hint or field factory."
            )
        return field

    @staticmethod
    def _normalize_hint(hint: object | None) -> object | None:
        module = getattr(hint, "__module__", None)
        name = getattr(hint, "__name__", None)
        if module == "structure.plugin.pyspark.dsl.field" and name == "float":
            return float
        if module == "structure.plugin.pyspark.dsl.field" and name == "date":
            return date
        return hint

    def _infer(self, schema: type, name: str, hint: object) -> StructureType:
        if hint is str:
            return String()
        if hint is bytes:
            return Binary()
        if hint is bool:
            return Boolean()
        if hint is int:
            return Integer()
        if hint is float:
            return Double()
        if hint is date:
            return Date()
        if hint is datetime:
            return Timestamp()
        if hint is Decimal:
            raise TypeError(f"{schema.__name__}.{name} uses Decimal; declare decimal(precision, scale) explicitly.")

        origin, args = get_origin(hint), get_args(hint)
        if origin in {Union, UnionType}:
            raise TypeError(f"{schema.__name__}.{name} does not support Optional or union schema hints. Use nullable=...")
        if origin is list:
            if len(args) != 1:
                raise TypeError(f"{schema.__name__}.{name} requires a parameterized list[T] schema hint.")
            return Array(self._infer(schema, name, args[0]))
        if origin is dict:
            if len(args) != 2:
                raise TypeError(f"{schema.__name__}.{name} requires a parameterized dict[K, V] schema hint.")
            return Map(self._infer(schema, name, args[0]), self._infer(schema, name, args[1]))
        if isinstance(hint, type) and issubclass(hint, Schema):
            return Struct(hint)
        raise TypeError(f"{schema.__name__}.{name} has unsupported schema hint {self._hint_text(hint)}.")

    def _compatible(self, hint: object, type: StructureType) -> bool:
        if hint is str:
            return isinstance(type, StringType)
        if hint is bytes:
            return isinstance(type, BinaryType)
        if hint is bool:
            return isinstance(type, BooleanType)
        if hint is int:
            return isinstance(type, (IntegerType, LongType))
        if hint is float:
            return isinstance(type, (FloatType, DoubleType))
        if hint is Decimal:
            return isinstance(type, DecimalType)
        if hint is date:
            return isinstance(type, DateType)
        if hint is datetime:
            return isinstance(type, TimestampType)

        origin, args = get_origin(hint), get_args(hint)
        if origin in {Union, UnionType}:
            return False
        if origin is list:
            return len(args) == 1 and isinstance(type, ArrayType) and self._compatible(args[0], type.element)
        if origin is dict:
            return (
                len(args) == 2
                and isinstance(type, MapType)
                and self._compatible(args[0], type.key)
                and self._compatible(args[1], type.value)
            )
        return isinstance(hint, builtins.type) and isinstance(type, StructType) and type.schema is hint

    def _resolve_nested(self, type: StructureType, resolving: set[type[Schema]]) -> None:
        if isinstance(type, ArrayType):
            self._resolve_nested(type.element, resolving)
        elif isinstance(type, MapType):
            self._resolve_nested(type.key, resolving)
            self._resolve_nested(type.value, resolving)
        elif isinstance(type, StructType):
            self._resolve(type.schema, type.schema._structure_fields, resolving)

    @staticmethod
    def _hint_text(hint: object) -> str:
        return getattr(hint, "__name__", repr(hint))

    @staticmethod
    def _type_text(type: StructureType) -> str:
        if isinstance(type, DecimalType):
            return f"decimal({type.precision}, {type.scale})"
        return type.name

    def _acyclic(self, root: type[Schema], type: object, path: tuple[type[Schema], ...]) -> None:
        if isinstance(type, ArrayType):
            self._acyclic(root, type.element, path)
        elif isinstance(type, MapType):
            self._acyclic(root, type.key, path)
            self._acyclic(root, type.value, path)
        elif isinstance(type, StructType):
            if type.schema in path:
                cycle = " -> ".join(schema.__name__ for schema in (*path, type.schema))
                raise ValueError(f"{root.__name__} has recursive Struct(...) schema composition: {cycle}. Nested Schema fields must form an acyclic schema graph.")
            for field in type.schema._structure_fields.values():
                self._acyclic(root, field.type, (*path, type.schema))
