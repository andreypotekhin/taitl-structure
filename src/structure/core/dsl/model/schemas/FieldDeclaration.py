from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from structure.core.dsl.model.schemas.FieldDefinition import FieldDefinition
from structure.core.dsl.model.types.StructureType import StructureType


@dataclass(frozen=True)
class FieldDeclaration:
    type: StructureType
    nullable: bool = True
    alias: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)
    description: str | None = None
    _options: frozenset[str] = field(default_factory=frozenset, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def definition(self, name: str) -> FieldDefinition:
        return FieldDefinition(
            name=name,
            type=self.type,
            nullable=self.nullable,
            alias=self.alias,
            metadata=self.metadata,
            description=self.description,
        )
