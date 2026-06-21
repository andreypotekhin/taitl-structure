from __future__ import annotations

from structure.app.dsl.logic.model.schemas.FieldDeclaration import FieldDeclaration
from structure.app.dsl.logic.model.schemas.FieldDefinition import FieldDefinition


class Structure:

    _structure_fields: dict[str, FieldDefinition] = {}
    _structure_local_fields: dict[str, FieldDefinition] = {}
    _structure_schema_bases: tuple[type["Structure"], ...] = ()

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        fields: dict[str, FieldDefinition] = {}
        for base in cls.__bases__:
            fields.update(getattr(base, "_structure_fields", {}))

        local_fields: dict[str, FieldDefinition] = {}
        for value in cls.__dict__.values():
            if isinstance(value, FieldDeclaration):
                definition = value.definition()
                local_fields[value.name] = definition
                fields[value.name] = definition

        cls._structure_fields = fields
        cls._structure_local_fields = local_fields
        cls._structure_schema_bases = tuple(
            base for base in cls.__bases__ if isinstance(base, type) and issubclass(base, Structure) and base is not Structure
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

        def build(**overrides: object) -> "Structure":
            base = cls._base_values(sources)
            base.update(overrides)
            return cls(**base)

        return build

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
        if isinstance(source, Structure):
            return source._structure_values.get(field, _MISSING)

        try:
            return getattr(source, field)
        except AttributeError:
            return _MISSING


_MISSING = object()
