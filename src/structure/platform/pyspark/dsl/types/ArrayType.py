from dataclasses import dataclass

from structure.platform.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class ArrayType(StructureType):
    element: StructureType
    contains_null: bool

    def __init__(self, element: StructureType, *, contains_null: bool = True) -> None:
        if not isinstance(element, StructureType):
            raise TypeError("Array(...) requires an explicit Structure type object such as String()")
        if not isinstance(contains_null, bool):
            raise TypeError("ArrayType contains_null must be a Boolean")
        object.__setattr__(self, "name", "array")
        object.__setattr__(self, "element", element)
        object.__setattr__(self, "contains_null", contains_null)
