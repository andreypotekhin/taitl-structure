from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.types.StructureType import StructureType


@dataclass(frozen=True)
class ArrayType(StructureType):
    element: StructureType
    contains_null: bool

    def __init__(self, element: StructureType, *, contains_null: bool = True) -> None:
        if not isinstance(contains_null, bool):
            raise TypeError("ArrayType contains_null must be a Boolean")
        object.__setattr__(self, "name", "array")
        object.__setattr__(self, "element", element)
        object.__setattr__(self, "contains_null", contains_null)
