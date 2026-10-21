from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.GeometryType import _validate_srid
from structure.plugin.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class GeographyType(StructureType):
    """Native Spark Geography, with ``None`` representing mixed or unknown SRIDs."""

    srid: int | None = None

    def __init__(self, srid: int | None = None) -> None:
        _validate_srid(srid)
        object.__setattr__(self, "name", "geography")
        object.__setattr__(self, "srid", srid)
