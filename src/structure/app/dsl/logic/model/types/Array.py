from structure.app.dsl.logic.model.types.ArrayType import ArrayType
from structure.app.dsl.logic.model.types.StructureType import StructureType


class Array(ArrayType):

    def __init__(self, element: StructureType, *, contains_null: bool = True) -> None:
        _require_type(element)
        super().__init__(element, contains_null=contains_null)


def _require_type(type: StructureType) -> None:
    if not isinstance(type, StructureType):
        raise TypeError("Array(...) requires an explicit Structure type object such as String()")
