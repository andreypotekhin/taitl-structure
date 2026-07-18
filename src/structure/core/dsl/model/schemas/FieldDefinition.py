from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Mapping

from structure.core.dsl.model.types.StructureType import StructureType


@dataclass(frozen=True)
class FieldDefinition:
    name: str
    type: StructureType
    nullable: bool = True
    alias: str | None = None
    metadata: Mapping[str, object] = dataclass_field(default_factory=dict)
    description: str | None = None

    @property
    def column(self) -> str:
        return self.alias or self.name
