"""Apache Sedona spatial helpers and its distinct Geometry field declaration."""

from typing import Any

from structure.plugin.pyspark.dsl.geo import spatial_call
from structure.plugin.pyspark.dsl.types import SedonaGeometryType


def geometry(srid: int | None = None, **options: object) -> Any:
    from structure.plugin.pyspark.dsl.field import _declare

    return _declare(SedonaGeometryType(srid), options)


def st_geomfromwkt(value: object, srid: int | object | None = None):
    return spatial_call("sedona", "st_geomfromwkt", value, srid) if srid is not None else spatial_call(
        "sedona", "st_geomfromwkt", value
    )


def st_geomfromwkb(value: object): return spatial_call("sedona", "st_geomfromwkb", value)
def st_geomfromewkb(value: object): return spatial_call("sedona", "st_geomfromewkb", value)
def st_astext(value: object): return spatial_call("sedona", "st_astext", value)
def st_asbinary(value: object): return spatial_call("sedona", "st_asbinary", value)
def st_asewkb(value: object): return spatial_call("sedona", "st_asewkb", value)
def st_srid(value: object): return spatial_call("sedona", "st_srid", value)
def st_setsrid(value: object, srid: object): return spatial_call("sedona", "st_setsrid", value, srid)
def st_intersects(left: object, right: object): return spatial_call("sedona", "st_intersects", left, right)
def st_contains(left: object, right: object): return spatial_call("sedona", "st_contains", left, right)
def st_within(left: object, right: object): return spatial_call("sedona", "st_within", left, right)


__all__ = [
    "geometry", "SedonaGeometryType", "st_geomfromwkt", "st_geomfromwkb", "st_geomfromewkb",
    "st_astext", "st_asbinary", "st_asewkb", "st_srid", "st_setsrid", "st_intersects",
    "st_contains", "st_within",
]
