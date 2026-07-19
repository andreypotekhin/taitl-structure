from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.types.Array import Array
from structure.core.dsl.model.types.ArrayType import ArrayType
from structure.core.dsl.model.types.Boolean import Boolean
from structure.core.dsl.model.types.BooleanType import BooleanType
from structure.core.dsl.model.types.Date import Date
from structure.core.dsl.model.types.DateType import DateType
from structure.core.dsl.model.types.Decimal import Decimal
from structure.core.dsl.model.types.DecimalType import DecimalType
from structure.core.dsl.model.types.Double import Double
from structure.core.dsl.model.types.DoubleType import DoubleType
from structure.core.dsl.model.types.Float import Float
from structure.core.dsl.model.types.FloatType import FloatType
from structure.core.dsl.model.types.Integer import Integer
from structure.core.dsl.model.types.IntegerType import IntegerType
from structure.core.dsl.model.types.Long import Long
from structure.core.dsl.model.types.LongType import LongType
from structure.core.dsl.model.types.Map import Map
from structure.core.dsl.model.types.MapType import MapType
from structure.core.dsl.model.types.String import String
from structure.core.dsl.model.types.StringType import StringType
from structure.core.dsl.model.types.Struct import Struct
from structure.core.dsl.model.types.StructType import StructType
from structure.core.dsl.model.types.StructureType import StructureType
from structure.core.dsl.model.types.Timestamp import Timestamp
from structure.core.dsl.model.types.TimestampType import TimestampType


def string() -> StructureType: return String()
def integer() -> StructureType: return Integer()
def long() -> StructureType: return Long()
def float() -> StructureType: return Float()
def double() -> StructureType: return Double()
def boolean() -> StructureType: return Boolean()
def date() -> StructureType: return Date()
def timestamp() -> StructureType: return Timestamp()
def decimal(precision: int, scale: int) -> StructureType: return Decimal(precision, scale)
def array(element: StructureType, *, contains_null: bool = True) -> StructureType: return Array(element, contains_null=contains_null)
def map(key: StructureType, value: StructureType, *, value_contains_null: bool = True) -> StructureType: return Map(key, value, value_contains_null=value_contains_null)
def struct(schema: type[Schema]) -> StructureType: return Struct(schema)


__all__ = [
    "ArrayType",
    "BooleanType",
    "DateType",
    "DecimalType",
    "DoubleType",
    "FloatType",
    "IntegerType",
    "LongType",
    "MapType",
    "StringType",
    "StructType",
    "StructureType",
    "TimestampType",
    "array",
    "boolean",
    "date",
    "decimal",
    "double",
    "float",
    "integer",
    "long",
    "map",
    "string",
    "struct",
    "timestamp",
]
