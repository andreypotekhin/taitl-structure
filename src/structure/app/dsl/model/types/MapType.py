from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.types.StructureType import StructureType


@dataclass(frozen=True)
class MapType(StructureType):
    key: StructureType
    value: StructureType
    value_contains_null: bool

    def __init__(self, key: StructureType, value: StructureType, *, value_contains_null: bool = True) -> None:
        if _contains_map(key):
            raise ValueError("MapType key cannot contain another MapType")
        object.__setattr__(self, "name", "map")
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "value_contains_null", value_contains_null)


def _contains_map(type: StructureType) -> bool:
    from structure.app.dsl.model.types.ArrayType import ArrayType
    from structure.app.dsl.model.types.StructType import StructType

    if isinstance(type, MapType):
        return True
    if isinstance(type, ArrayType):
        return _contains_map(type.element)
    if isinstance(type, StructType):
        return any(_contains_map(field.type) for field in type.schema._structure_fields.values())
    return False
