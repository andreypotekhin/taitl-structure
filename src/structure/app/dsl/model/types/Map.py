from structure.app.dsl.model.types.MapType import MapType
from structure.app.dsl.model.types.StructureType import StructureType


class Map(MapType):

    def __init__(self, key: StructureType, value: StructureType, *, value_contains_null: bool = True) -> None:
        _require_type(key)
        _require_type(value)
        super().__init__(key, value, value_contains_null=value_contains_null)


def _require_type(type: StructureType) -> None:
    if not isinstance(type, StructureType):
        raise TypeError("Map(...) requires explicit Structure type objects such as String()")
