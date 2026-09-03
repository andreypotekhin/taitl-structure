from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from structure.dsl import FieldDeclaration, FieldDefinition


@dataclass(frozen=True)
class IterableAnnotation:
    """Marks a field whose value type comes from its Python annotation."""


_ANNOTATION = IterableAnnotation()


def field(
    *,
    nullable: bool = True,
    alias: str | None = None,
    description: str | None = None,
    metadata: Mapping[str, object] | None = None,
) -> Any:
    """Declare Iterable field metadata while retaining the Python annotation as its type."""
    if not isinstance(nullable, bool):
        raise TypeError("Iterable field nullable must be a bool.")
    if alias is not None and (not isinstance(alias, str) or not alias):
        raise ValueError("Iterable field alias must be a non-empty string when supplied.")
    if description is not None and not isinstance(description, str):
        raise TypeError("Iterable field description must be a string or None.")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise TypeError("Iterable field metadata must be a mapping or None.")
    return FieldDeclaration(
        _ANNOTATION,
        nullable=nullable,
        alias=alias,
        description=description,
        metadata=metadata or {},
        validator=validate,
    )


def validate(schema: type, fields: Mapping[str, FieldDefinition]) -> None:
    columns: dict[str, str] = {}
    for definition in fields.values():
        if definition.type is _ANNOTATION and definition.hint is None:
            raise TypeError(f"{schema.__name__}.{definition.name} needs a Python type hint with Iterable field(...).")
        other = columns.get(definition.column)
        if other is not None:
            raise ValueError(
                f"{schema.__name__} has duplicate Iterable output key {definition.column!r}. "
                "Use a unique field alias."
            )
        columns[definition.column] = definition.name
