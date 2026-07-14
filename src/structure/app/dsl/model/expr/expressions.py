from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.types.ArrayType import ArrayType
from structure.app.dsl.model.types.BooleanType import BooleanType
from structure.app.dsl.model.types.DateType import DateType
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.DoubleType import DoubleType
from structure.app.dsl.model.types.FloatType import FloatType
from structure.app.dsl.model.types.IntegerType import IntegerType
from structure.app.dsl.model.types.LongType import LongType
from structure.app.dsl.model.types.MapType import MapType
from structure.app.dsl.model.types.StringType import StringType
from structure.app.dsl.model.types.StructType import StructType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.dsl.model.types.TimestampType import TimestampType


def literal(value: object) -> Expression:
    if isinstance(value, Expression):
        return value

    if isinstance(value, WhenBuilder):
        raise TypeError("when(...) must end with .otherwise(...) before it can be used as an expression")

    if isinstance(value, bool):
        return Expression(kind="literal", type=BooleanType(), nullable=False, data={"value": value})

    if isinstance(value, str):
        return Expression(kind="literal", type=StringType(), nullable=False, data={"value": value})

    if isinstance(value, int):
        type = IntegerType() if -(2**31) <= value <= 2**31 - 1 else LongType()
        return Expression(kind="literal", type=type, nullable=False, data={"value": value})

    if isinstance(value, float):
        return Expression(kind="literal", type=DoubleType(), nullable=False, data={"value": value})

    if isinstance(value, datetime):
        return Expression(kind="literal", type=TimestampType(), nullable=False, data={"value": value})

    if isinstance(value, date):
        return Expression(kind="literal", type=DateType(), nullable=False, data={"value": value})

    if value is None:
        return Expression(kind="literal", type=None, nullable=True, data={"value": None})

    return Expression(kind="literal", type=None, nullable=False, data={"value": value})


def lower(value: object) -> Expression:
    argument = literal(value)
    return Expression(
        kind="call", type=argument.type, nullable=argument.nullable, data={"function": "lower"}, args=(argument,)
    )


def trim(value: object) -> Expression:
    argument = literal(value)
    return Expression(
        kind="call", type=argument.type, nullable=argument.nullable, data={"function": "trim"}, args=(argument,)
    )


def upper(value: object) -> Expression:
    argument = literal(value)
    return Expression(
        kind="call", type=argument.type, nullable=argument.nullable, data={"function": "upper"}, args=(argument,)
    )


def substring(value: object, *, start: int, length: int) -> Expression:
    argument = _string_argument(value, "substring(...)")
    if isinstance(start, bool) or not isinstance(start, int) or start < 1:
        raise TypeError("substring(...) start must be a positive integer")
    if isinstance(length, bool) or not isinstance(length, int) or length < 0:
        raise TypeError("substring(...) length must be a non-negative integer")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "substring", "start": start, "length": length},
        args=(argument,),
    )


def split(value: object, *, pattern: str, limit: int = -1) -> Expression:
    argument = _string_argument(value, "split(...)")
    _string_literal(pattern, "split(...)", "pattern")
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise TypeError("split(...) limit must be an integer")
    return Expression(
        kind="call",
        type=ArrayType(StringType(), contains_null=False),
        nullable=argument.nullable,
        data={"function": "split", "pattern": pattern, "limit": limit},
        args=(argument,),
    )


def regexp_replace(value: object, *, pattern: str, replacement: str) -> Expression:
    argument = _string_argument(value, "regexp_replace(...)")
    _string_literal(pattern, "regexp_replace(...)", "pattern")
    _string_literal(replacement, "regexp_replace(...)", "replacement")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "regexp_replace", "pattern": pattern, "replacement": replacement},
        args=(argument,),
    )


def regexp_extract(value: object, *, pattern: str, group: int = 1) -> Expression:
    argument = _string_argument(value, "regexp_extract(...)")
    _string_literal(pattern, "regexp_extract(...)", "pattern")
    if isinstance(group, bool) or not isinstance(group, int) or group < 0:
        raise TypeError("regexp_extract(...) group must be a non-negative integer")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "regexp_extract", "pattern": pattern, "group": group},
        args=(argument,),
    )


def length(value: object) -> Expression:
    argument = _string_argument(value, "length(...)")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=argument.nullable,
        data={"function": "length"},
        args=(argument,),
    )


def initcap(value: object) -> Expression:
    return _string_call("initcap", value)


def reverse(value: object) -> Expression:
    return _string_call("reverse", value)


def translate(value: object, *, matching: str, replacement: str) -> Expression:
    argument = _string_argument(value, "translate(...)")
    _string_literal(matching, "translate(...)", "matching")
    _string_literal(replacement, "translate(...)", "replacement")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "translate", "matching": matching, "replacement": replacement},
        args=(argument,),
    )


def instr(value: object, *, substring: str) -> Expression:
    argument = _string_argument(value, "instr(...)")
    _string_literal(substring, "instr(...)", "substring")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=argument.nullable,
        data={"function": "instr", "substring": substring},
        args=(argument,),
    )


def levenshtein(left: object, right: object) -> Expression:
    left_argument = _string_argument(left, "levenshtein(...)")
    right_argument = _string_argument(right, "levenshtein(...)")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=left_argument.nullable or right_argument.nullable,
        data={"function": "levenshtein"},
        args=(left_argument, right_argument),
    )


def concat_ws(separator: str, *values: object) -> Expression:
    if not isinstance(separator, str):
        raise TypeError("concat_ws(...) separator must be a string literal")
    if not values:
        raise TypeError("concat_ws(...) requires at least one String value")
    arguments = tuple(_string_argument(value, "concat_ws(...)") for value in values)
    return Expression(
        kind="call",
        type=StringType(),
        nullable=False,
        data={"function": "concat_ws", "separator": separator},
        args=arguments,
    )


def date_add(value: object, *, days: int) -> Expression:
    argument = _date_or_timestamp_argument(value, "date_add(...)")
    if isinstance(days, bool) or not isinstance(days, int):
        raise TypeError("date_add(...) days must be an integer")
    return Expression(
        kind="call",
        type=DateType(),
        nullable=argument.nullable,
        data={"function": "date_add", "days": days},
        args=(argument,),
    )


def datediff(end: object, start: object) -> Expression:
    end_argument = _date_or_timestamp_argument(end, "datediff(...)")
    start_argument = _date_or_timestamp_argument(start, "datediff(...)")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=end_argument.nullable or start_argument.nullable,
        data={"function": "datediff"},
        args=(end_argument, start_argument),
    )


def date_trunc(value: object, *, unit: str) -> Expression:
    argument = _date_or_timestamp_argument(value, "date_trunc(...)")
    if not isinstance(unit, str) or not unit.strip():
        raise TypeError("date_trunc(...) unit must be a non-empty string literal")
    return Expression(
        kind="call",
        type=TimestampType(),
        nullable=argument.nullable,
        data={"function": "date_trunc", "unit": unit},
        args=(argument,),
    )


def abs(value: object) -> Expression:
    argument = _numeric_argument(value, "abs(...)")
    return Expression(
        kind="call", type=argument.type, nullable=argument.nullable, data={"function": "abs"}, args=(argument,)
    )


def round(value: object, *, scale: int = 0) -> Expression:
    argument = _numeric_argument(value, "round(...)")
    if isinstance(scale, bool) or not isinstance(scale, int):
        raise TypeError("round(...) scale must be an integer")
    return Expression(
        kind="call",
        type=argument.type,
        nullable=argument.nullable,
        data={"function": "round", "scale": scale},
        args=(argument,),
    )


def ceil(value: object) -> Expression:
    argument = _numeric_argument(value, "ceil(...)")
    return Expression(
        kind="call", type=_ceiling_type(argument.type), nullable=argument.nullable, data={"function": "ceil"}, args=(argument,)
    )


def floor(value: object) -> Expression:
    argument = _numeric_argument(value, "floor(...)")
    return Expression(
        kind="call", type=_ceiling_type(argument.type), nullable=argument.nullable, data={"function": "floor"}, args=(argument,)
    )


def isnull(value: object) -> Expression:
    return literal(value).is_null()


def isnotnull(value: object) -> Expression:
    return literal(value).is_not_null()


def isnan(value: object) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, (FloatType, DoubleType)):
        raise TypeError("isnan(...) requires a Float or Double Structure expression")
    return Expression(kind="is_nan", type=BooleanType(), nullable=False, args=(argument,))


def to_decimal(value: object, *, precision: int, scale: int) -> Expression:
    argument = literal(value)
    return Expression(
        kind="call",
        type=DecimalType(precision=precision, scale=scale),
        nullable=argument.nullable,
        data={"function": "to_decimal", "precision": precision, "scale": scale},
        args=(argument,),
    )


def coalesce(*values: object) -> Expression:
    if not values:
        raise TypeError("coalesce(...) requires at least one value")
    arguments = tuple(literal(value) for value in values)
    return Expression(
        kind="call",
        type=_common_type("coalesce(...)", arguments),
        nullable=all(argument.nullable for argument in arguments),
        data={"function": "coalesce"},
        args=arguments,
    )


def event_time_between(left: object, right: object, *, upper: str, lower: str = "0 seconds") -> Expression:
    left_argument = literal(left)
    right_argument = literal(right)
    if not isinstance(left_argument.type, TimestampType) or not isinstance(right_argument.type, TimestampType):
        raise TypeError("event_time_between(...) requires Timestamp Structure expressions")
    if not isinstance(lower, str) or not lower.strip():
        raise TypeError("event_time_between(lower=...) requires a non-empty string")
    if not isinstance(upper, str) or not upper.strip():
        raise TypeError("event_time_between(upper=...) requires a non-empty string")
    return Expression(
        kind="event_time_between",
        type=BooleanType(),
        nullable=left_argument.nullable or right_argument.nullable,
        data={"lower": lower, "upper": upper},
        args=(left_argument, right_argument),
    )


def when(condition: object, value: object) -> "WhenBuilder":
    predicate = literal(condition)
    if not isinstance(predicate.type, BooleanType):
        raise TypeError("when(...) requires a boolean Structure expression as its condition")
    return WhenBuilder(condition=predicate, value=literal(value))


@dataclass(frozen=True)
class WhenBuilder:
    condition: Expression
    value: Expression

    def otherwise(self, fallback: object) -> Expression:
        alternative = literal(fallback)
        return Expression(
            kind="when",
            type=_common_type("when(...).otherwise(...)", (self.value, alternative)),
            nullable=self.value.nullable or alternative.nullable,
            args=(self.condition, self.value, alternative),
        )


def _string_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, StringType):
        raise TypeError(f"{call} requires a String Structure expression")
    return argument


def _string_call(function: str, value: object) -> Expression:
    argument = _string_argument(value, f"{function}(...)")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": function},
        args=(argument,),
    )


def _string_literal(value: object, call: str, parameter: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{call} {parameter} must be a string literal")


def _date_or_timestamp_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, (DateType, TimestampType)):
        raise TypeError(f"{call} requires a Date or Timestamp Structure expression")
    return argument


def _numeric_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if argument.type is None or argument.type.name not in {"decimal", "double", "float", "integer", "long"}:
        raise TypeError(f"{call} requires a numeric Structure expression")
    return argument


def _common_type(call: str, arguments: tuple[Expression, ...]) -> StructureType | None:
    if any(argument.type is None and not argument.nullable for argument in arguments):
        raise TypeError(f"{call} requires typed Structure expressions or null literals")
    types = tuple(argument.type for argument in arguments if argument.type is not None)
    if not types:
        return None
    first = types[0]
    if all(_same_type(type, first) for type in types[1:]):
        return first
    if all(isinstance(type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)) for type in types):
        return _common_numeric_type(call, types)
    names = ", ".join(type.name for type in types)
    raise TypeError(f"{call} requires compatible types; received {names}")


def _common_numeric_type(call: str, types: tuple[StructureType, ...]) -> StructureType:
    decimals = tuple(type for type in types if isinstance(type, DecimalType))
    if decimals:
        if not all(isinstance(type, (IntegerType, LongType, DecimalType)) for type in types):
            names = ", ".join(type.name for type in types)
            raise TypeError(f"{call} requires compatible types; received {names}")
        scale = max(type.scale for type in decimals)
        integer_digits = max(
            type.precision - type.scale
            if isinstance(type, DecimalType)
            else 19
            if isinstance(type, LongType)
            else 10
            for type in types
        )
        precision = integer_digits + scale
        if precision > 38:
            raise TypeError(f"{call} cannot represent compatible Decimal values wider than precision 38")
        return DecimalType(precision=precision, scale=scale)
    if any(isinstance(type, DoubleType) for type in types):
        return DoubleType()
    if any(isinstance(type, FloatType) for type in types):
        return FloatType()
    if any(isinstance(type, LongType) for type in types):
        return LongType()
    return IntegerType()


def _same_type(left: StructureType, right: StructureType) -> bool:
    if left.name != right.name:
        return False
    if isinstance(left, ArrayType) and isinstance(right, ArrayType):
        return left.contains_null == right.contains_null and _same_type(left.element, right.element)
    if isinstance(left, MapType) and isinstance(right, MapType):
        return (
            left.value_contains_null == right.value_contains_null
            and _same_type(left.key, right.key)
            and _same_type(left.value, right.value)
        )
    if isinstance(left, StructType) and isinstance(right, StructType):
        return left.schema is right.schema
    if isinstance(left, DecimalType) and isinstance(right, DecimalType):
        return left.precision == right.precision and left.scale == right.scale
    return True


def _ceiling_type(type: object) -> StructureType:
    if isinstance(type, DecimalType):
        if type.scale == 0:
            return type
        return DecimalType(precision=type.precision - type.scale + 1, scale=0)
    return LongType()
