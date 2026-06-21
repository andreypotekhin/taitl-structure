from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.types.StructureType import StructureType


@dataclass(frozen=True)
class StringType(StructureType):
    def __init__(self) -> None:
        object.__setattr__(self, "name", "string")
