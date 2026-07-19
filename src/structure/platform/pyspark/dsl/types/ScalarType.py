from dataclasses import dataclass

from structure.platform.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class ScalarType(StructureType):
    def __init__(self, name: str) -> None:
        object.__setattr__(self, "name", name)
