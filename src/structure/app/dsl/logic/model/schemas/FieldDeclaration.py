from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping

from structure.app.dsl.logic.model.schemas.FieldDefinition import FieldDefinition
from structure.app.dsl.logic.model.types.StructureType import StructureType

if TYPE_CHECKING:
    from structure.app.dsl.logic.model.schemas.Structure import Structure


class FieldDeclaration:

    def __init__(
        self,
        type: StructureType,
        *,
        nullable: bool = True,
        primary_key: bool = False,
        metadata: Mapping[str, object] | None = None,
        description: str | None = None,
    ) -> None:
        self.type = type
        self.nullable = False if primary_key else nullable
        self.primary_key = primary_key
        self.metadata = MappingProxyType(dict(metadata or {}))
        self.description = description
        self.name = ""

    def __set_name__(self, owner: type[Structure], name: str) -> None:
        self.name = name

    def definition(self) -> FieldDefinition:
        return FieldDefinition(
            name=self.name,
            type=self.type,
            nullable=self.nullable,
            primary_key=self.primary_key,
            metadata=self.metadata,
            description=self.description,
        )
