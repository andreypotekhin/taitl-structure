from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class TimeType(StructureType):
    """Spark time-of-day type with precision from zero through six digits."""

    precision: int

    def __init__(self, precision: int = 6) -> None:
        if isinstance(precision, bool) or not isinstance(precision, int) or not 0 <= precision <= 6:
            raise ValueError("TIME precision must be an integer from 0 through 6")
        object.__setattr__(self, "name", "time")
        object.__setattr__(self, "precision", precision)
