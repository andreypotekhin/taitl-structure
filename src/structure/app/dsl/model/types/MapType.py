from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.types.StructureType import StructureType


@dataclass(frozen=True)
class MapType(StructureType):
    key: StructureType
    value: StructureType
    value_contains_null: bool

    def __init__(self, key: StructureType, value: StructureType, *, value_contains_null: bool = True) -> None:
        object.__setattr__(self, "name", "map")
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "value_contains_null", value_contains_null)
