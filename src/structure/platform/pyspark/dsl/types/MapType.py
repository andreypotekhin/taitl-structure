from dataclasses import dataclass

from structure.platform.pyspark.dsl.types.ContainsMap import contains_map
from structure.platform.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class MapType(StructureType):
    key: StructureType
    value: StructureType
    value_contains_null: bool

    def __init__(self, key: StructureType, value: StructureType, *, value_contains_null: object = True) -> None:
        if not isinstance(key, StructureType) or not isinstance(value, StructureType):
            raise TypeError("Map(...) requires explicit Structure type objects such as String()")
        if not isinstance(value_contains_null, bool):
            raise TypeError("MapType value_contains_null must be a Boolean")
        if contains_map(key):
            raise ValueError("MapType key cannot contain another MapType")
        object.__setattr__(self, "name", "map")
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "value_contains_null", value_contains_null)
