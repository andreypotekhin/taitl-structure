from __future__ import annotations

from structure.app.dsl.logic.model.schemas.FieldDeclaration import FieldDeclaration
from structure.app.dsl.logic.model.schemas.FieldDefinition import FieldDefinition


class Structure:

    _structure_fields: dict[str, FieldDefinition] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        fields: dict[str, FieldDefinition] = {}
        for base in cls.__bases__:
            fields.update(getattr(base, "_structure_fields", {}))

        for value in cls.__dict__.values():
            if isinstance(value, FieldDeclaration):
                fields[value.name] = value.definition()

        cls._structure_fields = fields

    def __init__(self, **values: object) -> None:
        unknown = set(values) - set(self._structure_fields)
        if unknown:
            allowed = ", ".join(self._structure_fields)
            raise TypeError(
                f"{type(self).__name__} got unknown field(s): {', '.join(sorted(unknown))}. Allowed: {allowed}"
            )
        self._structure_values = dict(values)
