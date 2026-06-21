from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.types.StructureType import StructureType


@dataclass(frozen=True)
class ArrayType(StructureType):
    element: StructureType
    contains_null: bool

    def __init__(self, element: StructureType, *, contains_null: bool = True) -> None:
        object.__setattr__(self, "name", "array")
        object.__setattr__(self, "element", element)
        object.__setattr__(self, "contains_null", contains_null)
