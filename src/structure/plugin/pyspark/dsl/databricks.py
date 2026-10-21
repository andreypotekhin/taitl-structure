"""Spatial helpers from ``pyspark.databricks.sql.functions``."""

from structure.plugin.pyspark.dsl.geo import spatial_call


def st_geomfromwkb(value: object, srid: int | object | None = None):
    if srid is None:
        return spatial_call("databricks", "st_geomfromwkb", value)
    return spatial_call("databricks", "st_geomfromwkb", value, srid)


def st_geogfromwkb(value: object): return spatial_call("databricks", "st_geogfromwkb", value)
def st_asbinary(value: object): return spatial_call("databricks", "st_asbinary", value)
def st_srid(value: object): return spatial_call("databricks", "st_srid", value)
def st_setsrid(value: object, srid: object): return spatial_call("databricks", "st_setsrid", value, srid)


__all__ = ["st_geomfromwkb", "st_geogfromwkb", "st_asbinary", "st_srid", "st_setsrid"]
