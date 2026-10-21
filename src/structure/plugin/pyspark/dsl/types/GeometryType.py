from dataclasses import dataclass

from structure.plugin.pyspark.dsl.types.StructureType import StructureType


@dataclass(frozen=True)
class GeometryType(StructureType):
    """Native Spark Geometry, with ``None`` representing mixed or unknown SRIDs."""

    srid: int | None = None

    def __init__(self, srid: int | None = None) -> None:
        _validate_srid(srid)
        object.__setattr__(self, "name", "geometry")
        object.__setattr__(self, "srid", srid)


def _validate_srid(srid: object) -> None:
    if srid is not None and (isinstance(srid, bool) or not isinstance(srid, int)):
        raise TypeError("Spatial SRID must be an integer or None")
