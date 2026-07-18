from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from math import isfinite
from re import fullmatch

from structure.core.dsl.model.expr.Expression import Expression
from structure.core.dsl.model.types.ArrayType import ArrayType
from structure.core.dsl.model.types.BooleanType import BooleanType
from structure.core.dsl.model.types.DateType import DateType
from structure.core.dsl.model.types.DecimalType import DecimalType
from structure.core.dsl.model.types.DoubleType import DoubleType
from structure.core.dsl.model.types.FloatType import FloatType
from structure.core.dsl.model.types.IntegerType import IntegerType
from structure.core.dsl.model.types.LongType import LongType
from structure.core.dsl.model.types.MapType import MapType
from structure.core.dsl.model.types.StringType import StringType
from structure.core.dsl.model.types.StructType import StructType
from structure.core.dsl.model.types.StructureType import StructureType
from structure.core.dsl.model.types.TimestampType import TimestampType


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

    if isinstance(value, Decimal):
        return Expression(kind="literal", type=_decimal_literal_type(value), nullable=False, data={"value": value})

    if isinstance(value, datetime):
        return Expression(kind="literal", type=TimestampType(), nullable=False, data={"value": value})

    if isinstance(value, date):
        return Expression(kind="literal", type=DateType(), nullable=False, data={"value": value})

    if value is None:
        return Expression(kind="literal", type=None, nullable=True, data={"value": None})

    return Expression(kind="literal", type=None, nullable=False, data={"value": value})


def _decimal_literal_type(value: Decimal) -> DecimalType:
    if not value.is_finite():
        raise TypeError("Decimal literals must be finite")
    digits = len(value.as_tuple().digits)
    exponent = value.as_tuple().exponent
    assert isinstance(exponent, int)
    scale = max(-exponent, 0)
    precision = max(digits + max(exponent, 0), scale)
    if precision > 38:
        raise TypeError("Decimal literals must not exceed Spark precision 38")
    return DecimalType(precision=precision, scale=scale)


def lower(value: object) -> Expression:
    return _string_call("lower", value)


def ltrim(value: object) -> Expression:
    return _string_call("ltrim", value)


def rtrim(value: object) -> Expression:
    return _string_call("rtrim", value)


def trim(value: object) -> Expression:
    return _string_call("trim", value)


def upper(value: object) -> Expression:
    return _string_call("upper", value)


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


def hash(*values: object) -> Expression:
    return _hash_call("hash", IntegerType(), values)


def xxhash64(*values: object) -> Expression:
    return _hash_call("xxhash64", LongType(), values)


def md5(value: object) -> Expression:
    return _string_call("md5", value)


def sha1(value: object) -> Expression:
    return _string_call("sha1", value)


def sha2(value: object, *, bits: int = 256) -> Expression:
    argument = _string_argument(value, "sha2(...)")
    if bits not in {224, 256, 384, 512}:
        raise TypeError("sha2(...) bits must be one of 224, 256, 384, or 512")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "sha2", "bits": bits},
        args=(argument,),
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


def date_sub(value: object, *, days: int) -> Expression:
    argument = _date_or_timestamp_argument(value, "date_sub(...)")
    if isinstance(days, bool) or not isinstance(days, int):
        raise TypeError("date_sub(...) days must be an integer")
    return Expression(
        kind="call",
        type=DateType(),
        nullable=argument.nullable,
        data={"function": "date_sub", "days": days},
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
    unit = _date_trunc_unit(unit)
    return Expression(
        kind="call",
        type=TimestampType(),
        nullable=argument.nullable,
        data={"function": "date_trunc", "unit": unit},
        args=(argument,),
    )


def trunc(value: object, *, unit: str) -> Expression:
    argument = _date_argument(value, "trunc(...)")
    unit = _trunc_unit(unit)
    return Expression(
        kind="call",
        type=DateType(),
        nullable=argument.nullable,
        data={"function": "trunc", "unit": unit},
        args=(argument,),
    )


def year(value: object) -> Expression:
    return _calendar_part("year", value, _date_or_timestamp_argument)


def month(value: object) -> Expression:
    return _calendar_part("month", value, _date_or_timestamp_argument)


def dayofmonth(value: object) -> Expression:
    return _calendar_part("dayofmonth", value, _date_or_timestamp_argument)


def hour(value: object) -> Expression:
    return _calendar_part("hour", value, _timestamp_argument)


def minute(value: object) -> Expression:
    return _calendar_part("minute", value, _timestamp_argument)


def second(value: object) -> Expression:
    return _calendar_part("second", value, _timestamp_argument)


def to_date(value: object, *, format: str | None = None) -> Expression:
    argument = _temporal_conversion_argument(value, "to_date(...)")
    format = _temporal_format(format, "to_date(...)")
    return Expression(
        kind="call",
        type=DateType(),
        nullable=True if isinstance(argument.type, StringType) else argument.nullable,
        data={"function": "to_date", **({"format": format} if format is not None else {})},
        args=(argument,),
    )


def to_timestamp(value: object, *, format: str | None = None) -> Expression:
    argument = _temporal_conversion_argument(value, "to_timestamp(...)")
    format = _temporal_format(format, "to_timestamp(...)")
    return Expression(
        kind="call",
        type=TimestampType(),
        nullable=True if isinstance(argument.type, StringType) else argument.nullable,
        data={"function": "to_timestamp", **({"format": format} if format is not None else {})},
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
    if argument.type is None:
        raise AssertionError("numeric argument validation must reject untyped expressions")
    return Expression(
        kind="call",
        type=_round_type(argument.type, scale),
        nullable=argument.nullable,
        data={"function": "round", "scale": scale},
        args=(argument,),
    )


def bround(value: object, *, scale: int = 0) -> Expression:
    argument = _numeric_argument(value, "bround(...)")
    if isinstance(scale, bool) or not isinstance(scale, int):
        raise TypeError("bround(...) scale must be an integer")
    if argument.type is None:
        raise AssertionError("numeric argument validation must reject untyped expressions")
    return Expression(
        kind="call",
        type=_round_type(argument.type, scale),
        nullable=argument.nullable,
        data={"function": "bround", "scale": scale},
        args=(argument,),
    )


def ceil(value: object) -> Expression:
    argument = _numeric_argument(value, "ceil(...)")
    return Expression(
        kind="call",
        type=_ceiling_type(argument.type),
        nullable=argument.nullable,
        data={"function": "ceil"},
        args=(argument,),
    )


def floor(value: object) -> Expression:
    argument = _numeric_argument(value, "floor(...)")
    return Expression(
        kind="call",
        type=_ceiling_type(argument.type),
        nullable=argument.nullable,
        data={"function": "floor"},
        args=(argument,),
    )


def sqrt(value: object) -> Expression:
    return _double_numeric_call("sqrt", value)


def pow(value: object, exponent: object) -> Expression:
    base = _numeric_argument(value, "pow(...)")
    power = _numeric_argument(exponent, "pow(...)")
    return Expression(
        kind="call",
        type=DoubleType(),
        nullable=base.nullable or power.nullable,
        data={"function": "pow"},
        args=(base, power),
    )


def log(value: object, *, base: float | int | None = None) -> Expression:
    argument = _numeric_argument(value, "log(...)")
    if base is None:
        return Expression(
            kind="call", type=DoubleType(), nullable=argument.nullable, data={"function": "log"}, args=(argument,)
        )
    if isinstance(base, bool) or not isinstance(base, (int, float)) or not isfinite(base) or base <= 0 or base == 1:
        raise TypeError("log(...) base must be a positive numeric literal other than 1")
    return Expression(
        kind="call",
        type=DoubleType(),
        nullable=argument.nullable,
        data={"function": "log", "base": base},
        args=(argument,),
    )


def exp(value: object) -> Expression:
    return _double_numeric_call("exp", value)


def signum(value: object) -> Expression:
    return _double_numeric_call("signum", value)


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
    argument = _decimal_argument(value)
    return Expression(
        kind="call",
        type=DecimalType(precision=precision, scale=scale),
        # Parsing and narrowing can fail for a present value, including values
        # outside the requested Decimal domain.
        nullable=True,
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


def nvl(value: object, fallback: object) -> Expression:
    return _null_fallback("nvl", value, fallback)


def ifnull(value: object, fallback: object) -> Expression:
    return _null_fallback("ifnull", value, fallback)


def nvl2(value: object, when_not_null: object, when_null: object) -> Expression:
    tested = literal(value)
    present = literal(when_not_null)
    missing = literal(when_null)
    return Expression(
        kind="call",
        type=_common_type("nvl2(...)", (present, missing)),
        nullable=present.nullable or missing.nullable,
        data={"function": "nvl2"},
        args=(tested, present, missing),
    )


def zeroifnull(value: object) -> Expression:
    argument = _numeric_argument(value, "zeroifnull(...)")
    if argument.type is None:
        raise AssertionError("numeric argument validation must reject untyped expressions")
    return Expression(
        kind="call", type=argument.type, nullable=False, data={"function": "zeroifnull"}, args=(argument,)
    )


def nullif(value: object, other: object) -> Expression:
    left = literal(value)
    right = literal(other)
    if left.type is None:
        raise TypeError("nullif(...) requires a typed left Structure expression")
    comparison = left == right
    if comparison.data is not None:
        raise TypeError("nullif(...) requires comparable Structure expression types")
    return Expression(
        kind="call",
        type=left.type,
        nullable=True,
        data={"function": "nullif"},
        args=(left, right),
    )


def nanvl(value: object, fallback: object) -> Expression:
    left = literal(value)
    right = literal(fallback)
    if not isinstance(left.type, (FloatType, DoubleType)) or not isinstance(right.type, (FloatType, DoubleType)):
        raise TypeError("nanvl(...) requires Float or Double Structure expressions")
    return Expression(
        kind="call",
        type=DoubleType(),
        nullable=left.nullable or right.nullable,
        data={"function": "nanvl"},
        args=(left, right),
    )


def event_time_between(left: object, right: object, *, upper: str, lower: str = "0 seconds") -> Expression:
    left_argument = literal(left)
    right_argument = literal(right)
    if not isinstance(left_argument.type, TimestampType) or not isinstance(right_argument.type, TimestampType):
        raise TypeError("event_time_between(...) requires Timestamp Structure expressions")
    lower = _nonnegative_interval(lower, "event_time_between(lower=...)")
    upper = _nonnegative_interval(upper, "event_time_between(upper=...)")
    if _interval_microseconds(lower) > _interval_microseconds(upper):
        raise TypeError("event_time_between(...) lower must not exceed upper")
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


def _null_fallback(function: str, value: object, fallback: object) -> Expression:
    arguments = (literal(value), literal(fallback))
    return Expression(
        kind="call",
        type=_common_type(f"{function}(...)", arguments),
        nullable=all(argument.nullable for argument in arguments),
        data={"function": function},
        args=arguments,
    )


def _hash_call(function: str, type: StructureType, values: tuple[object, ...]) -> Expression:
    if not values:
        raise TypeError(f"{function}(...) requires at least one scalar Structure expression")
    arguments = tuple(_hash_argument(value, f"{function}(...)") for value in values)
    return Expression(
        kind="call",
        type=type,
        nullable=any(argument.nullable for argument in arguments),
        data={"function": function},
        args=arguments,
    )


def _hash_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(
        argument.type,
        (BooleanType, StringType, IntegerType, LongType, FloatType, DoubleType, DecimalType, DateType, TimestampType),
    ):
        raise TypeError(f"{call} requires scalar Structure expressions")
    return argument


def _date_or_timestamp_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, (DateType, TimestampType)):
        raise TypeError(f"{call} requires a Date or Timestamp Structure expression")
    return argument


def _date_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, DateType):
        raise TypeError(f"{call} requires a Date Structure expression")
    return argument


def _timestamp_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, TimestampType):
        raise TypeError(f"{call} requires a Timestamp Structure expression")
    return argument


def _calendar_part(function: str, value: object, argument) -> Expression:
    source = argument(value, f"{function}(...)")
    return Expression(
        kind="call", type=IntegerType(), nullable=source.nullable, data={"function": function}, args=(source,)
    )


def _temporal_conversion_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, (StringType, DateType, TimestampType)):
        raise TypeError(f"{call} requires a String, Date, or Timestamp Structure expression")
    return argument


def _temporal_format(value: object, call: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise TypeError(f"{call} format must be a non-empty string literal")
    return value


def _numeric_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if argument.type is None or argument.type.name not in {"decimal", "double", "float", "integer", "long"}:
        raise TypeError(f"{call} requires a numeric Structure expression")
    return argument


def _double_numeric_call(function: str, value: object) -> Expression:
    argument = _numeric_argument(value, f"{function}(...)")
    return Expression(
        kind="call", type=DoubleType(), nullable=argument.nullable, data={"function": function}, args=(argument,)
    )


def _decimal_argument(value: object) -> Expression:
    argument = literal(value)
    if argument.type is None and argument.nullable:
        return argument
    if isinstance(argument.type, (StringType, IntegerType, LongType, FloatType, DoubleType, DecimalType, BooleanType)):
        return argument
    raise TypeError("to_decimal(...) requires a String, Boolean, or numeric Structure expression")


def _nonnegative_interval(value: object, call: str) -> str:
    if not isinstance(value, str) or not fullmatch(
        r"\s*\d+(?:\.\d+)?\s+(?:microseconds?|milliseconds?|seconds?|minutes?|hours?|days?|weeks?)\s*", value
    ):
        raise TypeError(f"{call} requires a non-negative fixed Spark interval string, such as '10 minutes'")
    return value.strip()


def _interval_microseconds(value: str) -> Decimal:
    match = fullmatch(r"(\d+(?:\.\d+)?)\s+(\w+)", value)
    if match is None:
        raise AssertionError("validated interval text must have an amount and unit")
    amount, unit = match.groups()
    multiplier = {
        "microsecond": 1,
        "microseconds": 1,
        "millisecond": 1_000,
        "milliseconds": 1_000,
        "second": 1_000_000,
        "seconds": 1_000_000,
        "minute": 60_000_000,
        "minutes": 60_000_000,
        "hour": 3_600_000_000,
        "hours": 3_600_000_000,
        "day": 86_400_000_000,
        "days": 86_400_000_000,
        "week": 604_800_000_000,
        "weeks": 604_800_000_000,
    }[unit]
    return Decimal(amount) * multiplier


def _date_trunc_unit(value: object) -> str:
    if not isinstance(value, str) or value.lower() not in _DATE_TRUNC_UNITS:
        raise TypeError(
            "date_trunc(...) unit must be one of year, yyyy, yy, quarter, month, mon, mm, week, day, dd, "
            "hour, minute, second, millisecond, or microsecond"
        )
    return value.lower()


def _trunc_unit(value: object) -> str:
    if not isinstance(value, str) or value.lower() not in _TRUNC_UNITS:
        raise TypeError("trunc(...) unit must be one of year, yyyy, yy, quarter, month, mon, mm, or week")
    return value.lower()


_DATE_TRUNC_UNITS = frozenset(
    {
        "year",
        "yyyy",
        "yy",
        "quarter",
        "month",
        "mon",
        "mm",
        "week",
        "day",
        "dd",
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
    }
)


_TRUNC_UNITS = frozenset({"year", "yyyy", "yy", "quarter", "month", "mon", "mm", "week"})


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
            type.precision - type.scale if isinstance(type, DecimalType) else 20 if isinstance(type, LongType) else 10
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


def _round_type(type: StructureType, scale: int) -> StructureType:
    if not isinstance(type, DecimalType):
        return type

    result_scale = min(type.scale, max(scale, 0))
    integral_digits = type.precision - type.scale + 1
    return DecimalType(precision=min(integral_digits + result_scale, 38), scale=result_scale)
