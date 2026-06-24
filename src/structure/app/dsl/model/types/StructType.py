from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.app.dsl.model.types.StructureType import StructureType

if TYPE_CHECKING:
    from structure.app.dsl.model.schemas.Structure import Structure


@dataclass(frozen=True)
class StructType(StructureType):
    schema: type[Structure]

    def __init__(self, schema: type[Structure]) -> None:
        object.__setattr__(self, "name", "struct")
        object.__setattr__(self, "schema", schema)
