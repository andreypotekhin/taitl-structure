from __future__ import annotations

from typing import ClassVar, get_origin, get_type_hints

from structure.core.dsl.model.schemas.FieldDeclaration import FieldDeclaration
from structure.core.dsl.model.schemas.FieldDefinition import ANNOTATION_TYPE, FieldDefinition


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
        annotations = cls.__dict__.get("__annotations__", {})
        hints = cls._local_hints(annotations)
        for name, value in cls.__dict__.items():
            if isinstance(value, FieldDeclaration):
                definition = value.definition(name, hints.get(name))
                local_fields[name] = definition
                fields[name] = definition

        for name, hint in hints.items():
            if name not in cls.__dict__:
                definition = FieldDefinition(name=name, type=ANNOTATION_TYPE, hint=hint)
                local_fields[name] = definition
                fields[name] = definition

        cls._validate_fields(fields)
        local_fields = {name: fields[name] for name in local_fields}
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
        from structure.plugin.pyspark.dsl.Projection import Projection

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
    def _validate_fields(cls, fields: dict[str, FieldDefinition]) -> None:
        validators = {field.validator for field in fields.values() if field.validator is not None}
        if len(validators) > 1:
            raise TypeError(
                f"{cls.__name__} combines field declarations from multiple plugins. "
                "Use one plugin's field DSL for each Schema."
            )
        if validators:
            validators.pop()(cls, fields)

    @classmethod
    def _local_hints(cls, annotations: object) -> dict[str, object]:
        if not isinstance(annotations, dict):
            return {}
        try:
            resolved = get_type_hints(cls)
        except (NameError, TypeError):
            resolved = annotations
        return {
            name: hint
            for name, hint in resolved.items()
            if name in annotations and get_origin(hint) is not ClassVar
        }


_MISSING = object()
