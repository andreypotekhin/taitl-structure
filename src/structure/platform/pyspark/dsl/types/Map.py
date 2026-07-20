from structure.platform.pyspark.dsl.types.MapType import MapType
from structure.platform.pyspark.dsl.types.StructureType import StructureType


class Map(MapType):
    def __init__(self, key: StructureType, value: StructureType, *, value_contains_null: object = True) -> None:
        super().__init__(key, value, value_contains_null=value_contains_null)
