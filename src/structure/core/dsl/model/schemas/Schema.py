from __future__ import annotations

from structure.core.dsl.model.schemas.FieldDeclaration import FieldDeclaration
from structure.core.dsl.model.schemas.FieldDefinition import FieldDefinition


class Schema:

    _structure_fields: dict[str, FieldDefinition] = {}
    _structure_local_fields: dict[str, FieldDefinition] = {}
    _structure_schema_bases: tuple[type["Schema"], ...] = ()

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        fields: dict[str, FieldDefinition] = {}
        for base in cls.__bases__:
            fields.update(getattr(base, "_structure_fields", {}))

        local_fields: dict[str, FieldDefinition] = {}
        for name, value in cls.__dict__.items():
            if isinstance(value, FieldDeclaration):
                definition = value.definition(name)
                local_fields[name] = definition
                fields[name] = definition

        cls._require_unique_columns(fields)
        cls._require_acyclic_structs(fields)
        cls._structure_fields = fields
        cls._structure_local_fields = local_fields
        cls._structure_schema_bases = tuple(
            base for base in cls.__bases__ if isinstance(base, type) and issubclass(base, Schema) and base is not Schema
        )

    def __init__(self, **values: object) -> None:
        unknown = set(values) - set(self._structure_fields)
        if unknown:
            allowed = ", ".join(self._structure_fields)
            raise TypeError(
                f"{type(self).__name__} got unknown field(s): {', '.join(sorted(unknown))}. Allowed: {allowed}"
            )
        self._structure_values = dict(values)

    @classmethod
    def base(cls, *sources: object):
        values = cls._base_values(sources)
        if set(values) == set(cls._structure_fields):
            return cls(**values)

        def build(**overrides: object) -> "Schema":
            base = cls._base_values(sources)
            base.update(overrides)
            return cls(**base)

        return build

    @classmethod
    def project(cls, source: object):
        from structure.core.dsl.model.schemas.Projection import Projection

        return Projection(source=source, target=cls)

    @classmethod
    def _base_values(cls, sources: tuple[object, ...]) -> dict[str, object]:
        values: dict[str, object] = {}
        for field in cls._structure_fields:
            for source in sources:
                value = cls._field_value(source, field)
                if value is not _MISSING:
                    values[field] = value
                    break
        return values

    @staticmethod
    def _field_value(source: object, field: str) -> object:
        if isinstance(source, Schema):
            return source._structure_values.get(field, _MISSING)

        try:
            return getattr(source, field)
        except AttributeError:
            return _MISSING

    @classmethod
    def _require_unique_columns(cls, fields: dict[str, FieldDefinition]) -> None:
        columns: dict[str, str] = {}
        for field in fields.values():
            other = columns.get(field.column)
            if other is not None:
                raise ValueError(
                    f"{cls.__name__} has duplicate Spark column name {field.column!r}. " "Use a unique field alias."
                )
            columns[field.column] = field.name

    @classmethod
    def _require_acyclic_structs(cls, fields: dict[str, FieldDefinition]) -> None:
        for field in fields.values():
            cls._require_acyclic_type(field.type, path=(cls,))

    @classmethod
    def _require_acyclic_type(cls, type, *, path: tuple[type["Schema"], ...]) -> None:
        from structure.core.dsl.model.types.ArrayType import ArrayType
        from structure.core.dsl.model.types.MapType import MapType
        from structure.core.dsl.model.types.StructType import StructType

        if isinstance(type, ArrayType):
            cls._require_acyclic_type(type.element, path=path)
            return
        if isinstance(type, MapType):
            cls._require_acyclic_type(type.key, path=path)
            cls._require_acyclic_type(type.value, path=path)
            return
        if not isinstance(type, StructType):
            return
        if type.schema in path:
            cycle = " -> ".join(schema.__name__ for schema in (*path, type.schema))
            raise ValueError(
                f"{cls.__name__} has recursive Struct(...) schema composition: {cycle}. "
                "Nested Schema fields must form an acyclic schema graph."
            )
        for field in type.schema._structure_fields.values():
            cls._require_acyclic_type(field.type, path=(*path, type.schema))


_MISSING = object()
