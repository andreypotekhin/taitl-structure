from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.core.dsl.model.types.StructureType import StructureType

if TYPE_CHECKING:
    from structure.core.dsl.model.schemas.Schema import Schema


@dataclass(frozen=True)
class StructType(StructureType):
    schema: type[Schema]

    def __init__(self, schema: type[Schema]) -> None:
        object.__setattr__(self, "name", "struct")
        object.__setattr__(self, "schema", schema)
