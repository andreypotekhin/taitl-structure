from __future__ import annotations

from typing import TYPE_CHECKING

from structure.plugin.pyspark.dsl.types.ArrayType import ArrayType
from structure.plugin.pyspark.dsl.types.Array import Array
from structure.plugin.pyspark.dsl.types.BinaryType import BinaryType
from structure.plugin.pyspark.dsl.types.Binary import Binary
from structure.plugin.pyspark.dsl.types.BooleanType import BooleanType
from structure.plugin.pyspark.dsl.types.Boolean import Boolean
from structure.plugin.pyspark.dsl.types.DateType import DateType
from structure.plugin.pyspark.dsl.types.Date import Date
from structure.plugin.pyspark.dsl.types.DecimalType import DecimalType
from structure.plugin.pyspark.dsl.types.Decimal import Decimal
from structure.plugin.pyspark.dsl.types.DoubleType import DoubleType
from structure.plugin.pyspark.dsl.types.Double import Double
from structure.plugin.pyspark.dsl.types.FloatType import FloatType
from structure.plugin.pyspark.dsl.types.Float import Float
from structure.plugin.pyspark.dsl.types.Geometry import Geometry
from structure.plugin.pyspark.dsl.types.GeometryType import GeometryType
from structure.plugin.pyspark.dsl.types.IntegerType import IntegerType
from structure.plugin.pyspark.dsl.types.Integer import Integer
from structure.plugin.pyspark.dsl.types.LongType import LongType
from structure.plugin.pyspark.dsl.types.Long import Long
from structure.plugin.pyspark.dsl.types.MapType import MapType
from structure.plugin.pyspark.dsl.types.Map import Map
from structure.plugin.pyspark.dsl.types.ScalarType import ScalarType
from structure.plugin.pyspark.dsl.types.StringType import StringType
from structure.plugin.pyspark.dsl.types.String import String
from structure.plugin.pyspark.dsl.types.StructType import StructType
from structure.plugin.pyspark.dsl.types.Struct import Struct
from structure.plugin.pyspark.dsl.types.StructureType import StructureType
from structure.plugin.pyspark.dsl.types.TimestampType import TimestampType
from structure.plugin.pyspark.dsl.types.Timestamp import Timestamp
from structure.plugin.pyspark.dsl.types.TimestampNTZType import TimestampNTZType
from structure.plugin.pyspark.dsl.types.TimestampNTZ import TimestampNTZ
from structure.plugin.pyspark.dsl.types.TimeType import TimeType
from structure.plugin.pyspark.dsl.types.Time import Time
from structure.plugin.pyspark.dsl.types.IntervalType import IntervalType
from structure.plugin.pyspark.dsl.Interval import Interval
from structure.plugin.pyspark.dsl.types.VariantType import VariantType
from structure.plugin.pyspark.dsl.types.Variant import Variant
from structure.plugin.pyspark.dsl.types.SketchType import BitmapType, HllSketchType, KllSketchType, SketchType, ThetaSketchType
from structure.plugin.pyspark.dsl.types.Bitmap import Bitmap
from structure.plugin.pyspark.dsl.types.HllSketch import HllSketch
from structure.plugin.pyspark.dsl.types.KllSketch import KllSketch
from structure.plugin.pyspark.dsl.types.ThetaSketch import ThetaSketch

if TYPE_CHECKING:
    from structure.dsl import Schema

def string() -> StructureType: return String()
def binary() -> StructureType: return Binary()
def integer() -> StructureType: return Integer()
def long() -> StructureType: return Long()
def float() -> StructureType: return Float()
def double() -> StructureType: return Double()
def boolean() -> StructureType: return Boolean()
def date() -> StructureType: return Date()
def timestamp() -> StructureType: return Timestamp()
def timestamp_ntz() -> StructureType: return TimestampNTZ()
def time(precision: int = 6) -> StructureType: return TimeType(precision)
def interval(*, type: str | None = None, unit: str | None = None) -> IntervalType:
    """Declare one exact Spark interval qualifier or unit."""
    if (type is None) == (unit is None):
        raise TypeError("types.interval(...) requires exactly one of type= or unit=")
    choice = type if type is not None else unit
    assert choice is not None
    if choice not in {item.value for item in Interval}:
        raise ValueError(f"Unsupported interval qualifier or unit: {choice!r}")
    if type is not None and choice not in {"year_to_month", "day_to_hour", "day_to_minute", "day_to_second", "hour_to_minute", "hour_to_second", "minute_to_second", "calendar"}:
        raise ValueError("types.interval(...) type= requires a compound qualifier or calendar; use unit= for one field")
    if unit is not None and choice == "calendar" or (unit is not None and "_to_" in choice):
        raise ValueError("types.interval(...) unit= requires one interval field")
    kind = "calendar" if choice == "calendar" else "year_month" if choice in {"year", "month", "year_to_month"} else "day_time"
    return IntervalType(kind, choice)
def variant() -> StructureType: return Variant()
def hll_sketch(*, profile: str = "baseline", lg_config_k: int = 12) -> StructureType: return HllSketch(profile, lg_config_k)
def bitmap(*, profile: str = "baseline") -> StructureType: return Bitmap(profile)
def kll_sketch(*, profile: str = "pyspark4.1") -> StructureType: return KllSketch(profile)
def theta_sketch(*, profile: str = "pyspark4.1") -> StructureType: return ThetaSketch(profile)
def geometry(srid: int) -> StructureType: return Geometry(srid)
def decimal(precision: int, scale: int) -> StructureType: return Decimal(precision, scale)
def array(element: StructureType, *, contains_null: object = True) -> StructureType: return Array(element, contains_null=contains_null)
def map(key: StructureType, value: StructureType, *, value_contains_null: object = True) -> StructureType: return Map(key, value, value_contains_null=value_contains_null)
def struct(schema: type[Schema]) -> StructureType: return Struct(schema)


__all__ = [
    "Array", "ArrayType", "Binary", "BinaryType", "Boolean", "BooleanType", "Date", "DateType", "Decimal", "DecimalType", "Double",
    "DoubleType", "Float", "FloatType", "Integer", "IntegerType", "Long", "LongType", "Map", "MapType",
    "Geometry", "GeometryType", "ScalarType", "SketchType", "HllSketch", "HllSketchType", "Bitmap", "BitmapType", "KllSketch", "KllSketchType", "ThetaSketch", "ThetaSketchType", "String", "StringType", "Struct", "StructType", "StructureType", "Time", "TimeType", "Timestamp", "TimestampType", "TimestampNTZ", "TimestampNTZType", "Variant", "VariantType",
    "array", "binary", "boolean", "date", "decimal", "double", "float", "geometry", "hll_sketch", "bitmap", "kll_sketch", "theta_sketch", "integer", "long", "map", "string", "struct", "time", "timestamp", "timestamp_ntz", "variant",
    "interval", "IntervalType",
]
