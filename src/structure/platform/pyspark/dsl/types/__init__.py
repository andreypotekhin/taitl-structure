from __future__ import annotations

from typing import TYPE_CHECKING

from structure.platform.pyspark.dsl.types.ArrayType import ArrayType
from structure.platform.pyspark.dsl.types.Array import Array
from structure.platform.pyspark.dsl.types.BooleanType import BooleanType
from structure.platform.pyspark.dsl.types.Boolean import Boolean
from structure.platform.pyspark.dsl.types.DateType import DateType
from structure.platform.pyspark.dsl.types.Date import Date
from structure.platform.pyspark.dsl.types.DecimalType import DecimalType
from structure.platform.pyspark.dsl.types.Decimal import Decimal
from structure.platform.pyspark.dsl.types.DoubleType import DoubleType
from structure.platform.pyspark.dsl.types.Double import Double
from structure.platform.pyspark.dsl.types.FloatType import FloatType
from structure.platform.pyspark.dsl.types.Float import Float
from structure.platform.pyspark.dsl.types.IntegerType import IntegerType
from structure.platform.pyspark.dsl.types.Integer import Integer
from structure.platform.pyspark.dsl.types.LongType import LongType
from structure.platform.pyspark.dsl.types.Long import Long
from structure.platform.pyspark.dsl.types.MapType import MapType
from structure.platform.pyspark.dsl.types.Map import Map
from structure.platform.pyspark.dsl.types.ScalarType import ScalarType
from structure.platform.pyspark.dsl.types.StringType import StringType
from structure.platform.pyspark.dsl.types.String import String
from structure.platform.pyspark.dsl.types.StructType import StructType
from structure.platform.pyspark.dsl.types.Struct import Struct
from structure.platform.pyspark.dsl.types.StructureType import StructureType
from structure.platform.pyspark.dsl.types.TimestampType import TimestampType
from structure.platform.pyspark.dsl.types.Timestamp import Timestamp

if TYPE_CHECKING:
    from structure.dsl import Schema

def string() -> StructureType: return String()
def integer() -> StructureType: return Integer()
def long() -> StructureType: return Long()
def float() -> StructureType: return Float()
def double() -> StructureType: return Double()
def boolean() -> StructureType: return Boolean()
def date() -> StructureType: return Date()
def timestamp() -> StructureType: return Timestamp()
def decimal(precision: int, scale: int) -> StructureType: return Decimal(precision, scale)
def array(element: StructureType, *, contains_null: object = True) -> StructureType: return Array(element, contains_null=contains_null)
def map(key: StructureType, value: StructureType, *, value_contains_null: object = True) -> StructureType: return Map(key, value, value_contains_null=value_contains_null)
def struct(schema: type[Schema]) -> StructureType: return Struct(schema)


__all__ = [
    "Array", "ArrayType", "Boolean", "BooleanType", "Date", "DateType", "Decimal", "DecimalType", "Double",
    "DoubleType", "Float", "FloatType", "Integer", "IntegerType", "Long", "LongType", "Map", "MapType",
    "ScalarType", "String", "StringType", "Struct", "StructType", "StructureType", "Timestamp", "TimestampType",
    "array", "boolean", "date", "decimal", "double", "float", "integer", "long", "map", "string", "struct", "timestamp",
]
