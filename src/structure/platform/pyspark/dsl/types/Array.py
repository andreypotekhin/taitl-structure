from structure.platform.pyspark.dsl.types.ArrayType import ArrayType
from structure.platform.pyspark.dsl.types.StructureType import StructureType


class Array(ArrayType):
    def __init__(self, element: StructureType, *, contains_null: bool = True) -> None:
        super().__init__(element, contains_null=contains_null)
