"""Standalone Structure type factories for casts and function contracts."""

from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.dsl.model.types.Array import Array
from structure.app.dsl.model.types.Boolean import Boolean
from structure.app.dsl.model.types.Date import Date
from structure.app.dsl.model.types.Decimal import Decimal
from structure.app.dsl.model.types.Double import Double
from structure.app.dsl.model.types.Float import Float
from structure.app.dsl.model.types.Integer import Integer
from structure.app.dsl.model.types.Long import Long
from structure.app.dsl.model.types.Map import Map
from structure.app.dsl.model.types.String import String
from structure.app.dsl.model.types.Struct import Struct
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.dsl.model.types.Timestamp import Timestamp


def string() -> StructureType:
    return String()


def integer() -> StructureType:
    return Integer()


def long() -> StructureType:
    return Long()


def float() -> StructureType:
    return Float()


def double() -> StructureType:
    return Double()


def boolean() -> StructureType:
    return Boolean()


def date() -> StructureType:
    return Date()


def timestamp() -> StructureType:
    return Timestamp()


def decimal(precision: int, scale: int) -> StructureType:
    return Decimal(precision, scale)


def array(element: StructureType, *, contains_null: bool = True) -> StructureType:
    return Array(element, contains_null=contains_null)


def map(key: StructureType, value: StructureType, *, value_contains_null: bool = True) -> StructureType:
    return Map(key, value, value_contains_null=value_contains_null)


def struct(schema: type[Schema]) -> StructureType:
    return Struct(schema)


__all__ = [
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
