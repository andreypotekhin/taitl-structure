"""Small runtime checks used by generated schema modules."""


def native_spatial_type(types, kind: str, srid: int | None):
    constructor = getattr(types, kind, None)
    if constructor is None:
        label = "Geometry" if kind == "GeometryType" else "Geography"
        raise RuntimeError(
            f"Native Spark {label} schema types are unavailable on this runtime. "
            "Use a target with native spatial type constructors; see docs/reference/Geospatial.ref.md."
        )
    return constructor(srid if srid is not None else "ANY")


def sedona_geometry_type(srid: int | None):
    try:
        from sedona.spark.sql.types import GeometryType  # type: ignore[import-not-found]
    except ImportError as error:
        raise RuntimeError(
            "Sedona Geometry schema requires Apache Sedona; install the matching Sedona Python and JVM packages. "
            "See docs/reference/Geospatial.ref.md."
        ) from error
    return GeometryType()
