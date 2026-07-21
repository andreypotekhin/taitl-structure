from structure.plugin.pyspark.dsl.types.ArrayType import ArrayType
from structure.plugin.pyspark.dsl.types.StructureType import StructureType


class Array(ArrayType):
    def __init__(self, element: StructureType, *, contains_null: object = True) -> None:
        super().__init__(element, contains_null=contains_null)
