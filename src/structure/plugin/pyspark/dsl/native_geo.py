"""Apache PySpark native spatial helpers."""

from structure.plugin.pyspark.dsl.geo import spatial_call


def st_geomfromwkb(value: object): return spatial_call("apache", "st_geomfromwkb", value, default_srid=0)
def st_geogfromwkb(value: object): return spatial_call("apache", "st_geogfromwkb", value)
def st_asbinary(value: object): return spatial_call("apache", "st_asbinary", value)
def st_srid(value: object): return spatial_call("apache", "st_srid", value)
def st_setsrid(value: object, srid: object): return spatial_call("apache", "st_setsrid", value, srid)


__all__ = ["st_geomfromwkb", "st_geogfromwkb", "st_asbinary", "st_srid", "st_setsrid"]
