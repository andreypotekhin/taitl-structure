from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class StringType(StructureType):
    def __init__(self) -> None:
        object.__setattr__(self, "name", "string")
