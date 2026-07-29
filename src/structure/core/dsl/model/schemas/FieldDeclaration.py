"""Intermediate field declarations produced by plugin field factories."""

from __future__ import annotations

from builtins import type as class_type
from collections.abc import Callable
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping

from structure.core.dsl.model.schemas.FieldDefinition import FieldDefinition


@dataclass(frozen=True)
class FieldDeclaration:
    """A field before Python assigns its final schema attribute name.

    Plugin field factories return this object so ``Schema.__init_subclass__``
    can attach the field name, annotation, alias, metadata, and plugin
    validator in one immutable :class:`FieldDefinition`.
    """

    type: Any
    nullable: bool = True
    alias: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)
    description: str | None = None
    validator: Callable[[class_type, Mapping[str, "FieldDefinition"]], None] | None = field(
        default=None, repr=False, compare=False
    )
    _options: frozenset[str] = field(default_factory=frozenset, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def definition(self, name: str, hint: object | None = None) -> FieldDefinition:
        """Create the finalized field definition for a schema attribute."""
        return FieldDefinition(
            name=name,
            type=self.type,
            hint=hint,
            nullable=self.nullable,
            alias=self.alias,
            metadata=self.metadata,
            description=self.description,
            validator=self.validator,
        )
