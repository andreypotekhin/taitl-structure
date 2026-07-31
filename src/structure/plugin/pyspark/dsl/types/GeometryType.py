from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class GeometryType(StructureType):
    """A planar Spark SQL Geometry value with a declared coordinate reference."""

    srid: int

    def __init__(self, srid: int) -> None:
        if isinstance(srid, bool) or not isinstance(srid, int) or srid <= 0:
            raise ValueError("Geometry SRID must be a positive integer")
        object.__setattr__(self, "name", "geometry")
        object.__setattr__(self, "srid", srid)
