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
from structure.plugin.pyspark.dsl.types.VariantType import VariantType
from structure.plugin.pyspark.dsl.types.Variant import Variant

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
def variant() -> StructureType: return Variant()
def geometry(srid: int) -> StructureType: return Geometry(srid)
def decimal(precision: int, scale: int) -> StructureType: return Decimal(precision, scale)
def array(element: StructureType, *, contains_null: object = True) -> StructureType: return Array(element, contains_null=contains_null)
def map(key: StructureType, value: StructureType, *, value_contains_null: object = True) -> StructureType: return Map(key, value, value_contains_null=value_contains_null)
def struct(schema: type[Schema]) -> StructureType: return Struct(schema)


__all__ = [
    "Array", "ArrayType", "Binary", "BinaryType", "Boolean", "BooleanType", "Date", "DateType", "Decimal", "DecimalType", "Double",
    "DoubleType", "Float", "FloatType", "Integer", "IntegerType", "Long", "LongType", "Map", "MapType",
    "Geometry", "GeometryType", "ScalarType", "String", "StringType", "Struct", "StructType", "StructureType", "Timestamp", "TimestampType", "Variant", "VariantType",
    "array", "binary", "boolean", "date", "decimal", "double", "float", "geometry", "integer", "long", "map", "string", "struct", "timestamp", "variant",
]
