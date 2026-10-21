from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.GeometryType import _validate_srid
from structure.plugin.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class SedonaGeometryType(StructureType):
    """Apache Sedona's Geometry UDT, distinct from Spark's native Geometry."""

    srid: int | None = None

    def __init__(self, srid: int | None = None) -> None:
        _validate_srid(srid)
        object.__setattr__(self, "name", "sedona_geometry")
        object.__setattr__(self, "srid", srid)
