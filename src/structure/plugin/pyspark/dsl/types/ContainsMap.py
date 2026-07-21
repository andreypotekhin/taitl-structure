from structure.plugin.pyspark.dsl.types.ArrayType import ArrayType
from structure.plugin.pyspark.dsl.types.StructureType import StructureType


def contains_map(type_: StructureType) -> bool:
    from structure.plugin.pyspark.dsl.types.MapType import MapType
    from structure.plugin.pyspark.dsl.types.StructType import StructType

    if isinstance(type_, MapType):
        return True
    if isinstance(type_, ArrayType):
        return contains_map(type_.element)
    if isinstance(type_, StructType):
        return any(contains_map(field.type) for field in type_.schema._structure_fields.values())
    return False
