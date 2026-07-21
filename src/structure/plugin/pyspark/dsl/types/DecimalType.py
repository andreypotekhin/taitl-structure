from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class DecimalType(StructureType):
    precision: int
    scale: int

    def __init__(self, precision: int, scale: int) -> None:
        if isinstance(precision, bool) or not isinstance(precision, int) or not 1 <= precision <= 38:
            raise ValueError("Decimal precision must be an integer from 1 through 38")
        if isinstance(scale, bool) or not isinstance(scale, int) or not 0 <= scale <= precision:
            raise ValueError("Decimal scale must be an integer from 0 through precision")
        object.__setattr__(self, "name", "decimal")
        object.__setattr__(self, "precision", precision)
        object.__setattr__(self, "scale", scale)
