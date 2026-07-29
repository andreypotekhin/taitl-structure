"""PySpark-compatible scalar expression helpers.

The functions in this module mirror common ``pyspark.sql.functions`` behavior
while returning Structure :class:`Expression` objects instead of Spark
``Column`` objects.  They validate types at authoring time so generated Spark
code is predictable and failures point to the DSL call that caused them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from math import isfinite
from re import fullmatch
from typing import Any, Mapping

from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructType,
    StructureType,
    TimestampType,
)

__all__ = [
    "abs", "base64", "bround", "ceil", "coalesce", "concat_ws", "date_add", "date_sub", "date_trunc", "datediff",
    "dayofmonth", "event_time_between", "exp", "floor", "from_csv", "from_json", "hash", "hour", "ifnull", "initcap",
    "instr", "isnan", "isnotnull", "isnull", "CsvOptions", "JsonOptions", "length", "levenshtein", "literal", "log",
    "lower", "ltrim", "md5",
    "minute", "month", "nanvl", "nullif", "nvl", "nvl2", "pow", "regexp_extract", "regexp_replace", "reverse",
    "round", "rtrim", "sha1", "sha2", "second", "signum", "split", "sqrt", "substring", "to_csv", "to_date",
    "to_decimal", "to_json", "to_timestamp", "translate", "trim", "trunc", "unbase64", "decode", "encode", "upper",
    "when", "xxhash64", "year", "zeroifnull",
]


@dataclass(frozen=True)
class JsonOptions:
    """Literal JSON parser/renderer options for ``from_json`` and ``to_json``.

    Args:
        null_value: String token Spark treats as null.
        date_format: Java date pattern used by Spark.
        timestamp_format: Java timestamp pattern used by Spark.
        mode: Parsing mode. V7 admits only ``"PERMISSIVE"``.

    Returns:
        An immutable option record accepted by JSON conversion helpers.

    Example:
        options = JsonOptions(date_format="yyyy-MM-dd")
    """

    null_value: str | None = None
    date_format: str | None = None
    timestamp_format: str | None = None
    mode: str = "PERMISSIVE"

    def spark_options(self, *, writer: bool = False) -> dict[str, str]:
        """Return normalized Spark option names and values."""
        return _spark_options(
            self,
            writer=writer,
            keys={
                "null_value": "nullValue",
                "date_format": "dateFormat",
                "timestamp_format": "timestampFormat",
                "mode": "mode",
            },
        )


@dataclass(frozen=True)
class CsvOptions:
    """Literal CSV parser/renderer options for ``from_csv`` and ``to_csv``.

    Args:
        delimiter: One-character field delimiter, rendered as Spark ``sep``.
        quote: One-character quote marker.
        escape: One-character escape marker.
        null_value: String token Spark treats as null.
        date_format: Java date pattern used by Spark.
        timestamp_format: Java timestamp pattern used by Spark.
        mode: Parsing mode. V7 admits only ``"PERMISSIVE"``.

    Returns:
        An immutable option record accepted by CSV conversion helpers.

    Example:
        options = CsvOptions(delimiter="|", null_value="")
    """

    delimiter: str | None = None
    quote: str | None = None
    escape: str | None = None
    null_value: str | None = None
    date_format: str | None = None
    timestamp_format: str | None = None
    mode: str = "PERMISSIVE"

    def spark_options(self, *, writer: bool = False) -> dict[str, str]:
        """Return normalized Spark option names and values."""
        return _spark_options(
            self,
            writer=writer,
            keys={
                "delimiter": "sep",
                "quote": "quote",
                "escape": "escape",
                "null_value": "nullValue",
                "date_format": "dateFormat",
                "timestamp_format": "timestampFormat",
                "mode": "mode",
            },
        )


def literal(value: object) -> Expression:
    """Convert a Python value or Structure object into a symbolic expression.

    Args:
        value: A Python literal, Structure schema instance, existing
            ``Expression``, or null.

    Returns:
        A typed expression whenever Structure can infer the Spark type.

    Examples:
        literal("paid")
        literal(Decimal("10.50"))
        literal(None)
    """
    if isinstance(value, Expression):
        return value

    if isinstance(value, WhenBuilder):
        raise TypeError("when(...) must end with .otherwise(...) before it can be used as an expression")

    if hasattr(value, "_structure_fields") and hasattr(value, "_structure_values"):
        schema = value.__class__
        fields = tuple(schema._structure_fields.values())
        values = value._structure_values
        return Expression(
            kind="struct",
            type=StructType(schema),
            nullable=False,
            data={"fields": fields},
            args=tuple(literal(values[field.name]) for field in fields),
        )

    if isinstance(value, bool):
        return Expression(kind="literal", type=BooleanType(), nullable=False, data={"value": value})

    if isinstance(value, str):
        return Expression(kind="literal", type=StringType(), nullable=False, data={"value": value})

    if isinstance(value, (bytes, bytearray)):
        return Expression(kind="literal", type=BinaryType(), nullable=False, data={"value": bytes(value)})

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
    """Lowercase a string expression, like Spark ``lower``."""
    return _string_call("lower", value)


def ltrim(value: object) -> Expression:
    """Trim leading whitespace from a string expression, like Spark ``ltrim``."""
    return _string_call("ltrim", value)


def rtrim(value: object) -> Expression:
    """Trim trailing whitespace from a string expression, like Spark ``rtrim``."""
    return _string_call("rtrim", value)


def trim(value: object) -> Expression:
    """Trim leading and trailing whitespace from a string expression."""
    return _string_call("trim", value)


def upper(value: object) -> Expression:
    """Uppercase a string expression, like Spark ``upper``."""
    return _string_call("upper", value)


def base64(value: object) -> Expression:
    """Encode binary data as Base64 text, like PySpark ``base64``.

    Args:
        value: Binary Structure expression or Python ``bytes`` literal.

    Returns:
        A nullable string expression containing Base64 text.

    Example:
        token_text = base64(raw.token_bytes)
    """
    argument = _binary_argument(value, "base64(...)")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "base64"},
        args=(argument,),
    )


def unbase64(value: object) -> Expression:
    """Decode Base64 text into binary data, like PySpark ``unbase64``.

    Args:
        value: String Structure expression or Python string literal.

    Returns:
        A nullable binary expression.

    Example:
        token_bytes = unbase64(raw.token_text)
    """
    argument = _string_argument(value, "unbase64(...)")
    return Expression(
        kind="call",
        type=BinaryType(),
        nullable=True,
        data={"function": "unbase64"},
        args=(argument,),
    )


def encode(value: object, *, charset: str = "UTF-8") -> Expression:
    """Encode text into binary data, like PySpark ``encode``.

    Args:
        value: String Structure expression or Python string literal.
        charset: Non-empty Java charset name accepted by Spark.

    Returns:
        A nullable binary expression.

    Example:
        payload = encode(raw.text, charset="UTF-8")
    """
    argument = _string_argument(value, "encode(...)")
    return Expression(
        kind="call",
        type=BinaryType(),
        nullable=argument.nullable,
        data={"function": "encode", "charset": _charset(charset, "encode(...)")},
        args=(argument,),
    )


def decode(value: object, *, charset: str = "UTF-8") -> Expression:
    """Decode binary data into text, like PySpark ``decode``.

    Args:
        value: Binary Structure expression or Python ``bytes`` literal.
        charset: Non-empty Java charset name accepted by Spark.

    Returns:
        A nullable string expression.

    Example:
        text = decode(raw.payload, charset="UTF-8")
    """
    argument = _binary_argument(value, "decode(...)")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "decode", "charset": _charset(charset, "decode(...)")},
        args=(argument,),
    )


def from_json(value: object, *, as_: type, options: JsonOptions = JsonOptions()) -> Expression:
    """Parse JSON text into a declared Structure record, like PySpark ``from_json``.

    Args:
        value: String Structure expression or Python string literal containing JSON.
        as_: ``Schema`` class that declares the parsed struct shape.
        options: Immutable JSON parser options.

    Returns:
        A nullable struct expression with the exact declared ``as_`` schema.

    Example:
        payload = from_json(raw.payload_json, as_=Payload)
    """
    argument = _string_argument(value, "from_json(...)")
    schema = _parser_schema_argument(as_, "from_json(...)")
    return Expression(
        kind="call",
        type=StructType(schema),
        nullable=True,
        data={"function": "from_json", "schema": schema, "options": _json_options(options).spark_options()},
        args=(argument,),
    )


def to_json(value: object, *, options: JsonOptions = JsonOptions()) -> Expression:
    """Render a struct expression as JSON text, like PySpark ``to_json``.

    Args:
        value: Struct Structure expression.
        options: Immutable JSON rendering options.

    Returns:
        A nullable string expression.

    Example:
        payload_json = to_json(row.payload)
    """
    argument = _struct_argument(value, "to_json(...)")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=True,
        data={"function": "to_json", "options": _json_options(options).spark_options(writer=True)},
        args=(argument,),
    )


def from_csv(value: object, *, as_: type, options: CsvOptions = CsvOptions()) -> Expression:
    """Parse CSV text into a declared Structure record, like PySpark ``from_csv``.

    Args:
        value: String Structure expression or Python string literal containing one CSV row.
        as_: ``Schema`` class that declares the parsed struct shape.
        options: Immutable CSV parser options.

    Returns:
        A nullable struct expression with the exact declared ``as_`` schema.

    Example:
        payload = from_csv(raw.payload_csv, as_=Payload, options=CsvOptions(delimiter="|"))
    """
    argument = _string_argument(value, "from_csv(...)")
    schema = _parser_schema_argument(as_, "from_csv(...)")
    return Expression(
        kind="call",
        type=StructType(schema),
        nullable=True,
        data={"function": "from_csv", "schema": schema, "options": _csv_options(options).spark_options()},
        args=(argument,),
    )


def to_csv(value: object, *, options: CsvOptions = CsvOptions()) -> Expression:
    """Render a struct expression as CSV text, like PySpark ``to_csv``.

    Args:
        value: Struct Structure expression.
        options: Immutable CSV rendering options.

    Returns:
        A nullable string expression.

    Example:
        payload_csv = to_csv(row.payload, options=CsvOptions(delimiter="|"))
    """
    argument = _struct_argument(value, "to_csv(...)")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=True,
        data={"function": "to_csv", "options": _csv_options(options).spark_options(writer=True)},
        args=(argument,),
    )


def substring(value: object, *, start: int, length: int) -> Expression:
    """Return a substring expression using Spark's one-based indexing.

    Args:
        value: String expression.
        start: One-based starting position, matching PySpark ``substring``.
        length: Number of characters to return.

    Returns:
        A nullable string expression.

    Example:
        short_code = substring(order.code, start=1, length=3)
    """
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
    """Split a string expression into an array of non-null strings."""
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
    """Replace text matched by a regular expression."""
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
    """Extract a regex capture group as a string expression."""
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
    """Return the length of a string expression."""
    argument = _string_argument(value, "length(...)")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=argument.nullable,
        data={"function": "length"},
        args=(argument,),
    )


def initcap(value: object) -> Expression:
    """Title-case words in a string expression, like Spark ``initcap``."""
    return _string_call("initcap", value)


def reverse(value: object) -> Expression:
    """Reverse a string expression, like Spark ``reverse`` for strings."""
    return _string_call("reverse", value)


def translate(value: object, *, matching: str, replacement: str) -> Expression:
    """Translate characters in a string expression."""
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
    """Return the one-based position of a substring, like Spark ``instr``."""
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
    """Return the Levenshtein distance between two string expressions."""
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
    """Concatenate string or ``array<string>`` expressions with a separator."""
    if not isinstance(separator, str):
        raise TypeError("concat_ws(...) separator must be a string literal")
    if not values:
        raise TypeError("concat_ws(...) requires at least one String value")
    arguments = tuple(_concat_ws_argument(value) for value in values)
    return Expression(
        kind="call",
        type=StringType(),
        nullable=False,
        data={"function": "concat_ws", "separator": separator},
        args=arguments,
    )


def hash(*values: object) -> Expression:
    """Return Spark's 32-bit hash for one or more scalar expressions."""
    return _hash_call("hash", IntegerType(), values)


def xxhash64(*values: object) -> Expression:
    """Return Spark's 64-bit xxHash for one or more scalar expressions."""
    return _hash_call("xxhash64", LongType(), values)


def md5(value: object) -> Expression:
    """Return the MD5 hex digest for a string expression."""
    return _string_call("md5", value)


def sha1(value: object) -> Expression:
    """Return the SHA-1 hex digest for a string expression."""
    return _string_call("sha1", value)


def sha2(value: object, *, bits: int = 256) -> Expression:
    """Return a SHA-2 hex digest for a string expression."""
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


def date_add(value: object, *, days: object) -> Expression:
    """Add whole days to a Date or Timestamp expression.

    Args:
        value: Date or Timestamp expression.
        days: Integer literal or integral Structure expression.

    Returns:
        A Date expression, following PySpark ``date_add``.

    Example:
        ship_date = date_add(order.created_at, days=2)
    """
    argument = _date_or_timestamp_argument(value, "date_add(...)")
    if isinstance(days, bool):
        raise TypeError("date_add(...) days must be an integer or integral Structure expression")
    if isinstance(days, int):
        return Expression(
            kind="call",
            type=DateType(),
            nullable=argument.nullable,
            data={"function": "date_add", "days": days},
            args=(argument,),
        )
    day_count = literal(days)
    if not isinstance(day_count.type, (IntegerType, LongType)):
        raise TypeError("date_add(...) days must be an integer or integral Structure expression")
    return Expression(
        kind="call",
        type=DateType(),
        nullable=argument.nullable or day_count.nullable,
        data={"function": "date_add"},
        args=(argument, day_count),
    )


def date_sub(value: object, *, days: int) -> Expression:
    """Subtract whole days from a Date or Timestamp expression."""
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
    """Return the day difference between two Date or Timestamp expressions."""
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
    """Truncate a Date or Timestamp expression to a supported Spark unit."""
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
    """Truncate a Date expression to a supported Spark date unit."""
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
    """Extract the year from a Date or Timestamp expression."""
    return _calendar_part("year", value, _date_or_timestamp_argument)


def month(value: object) -> Expression:
    """Extract the month from a Date or Timestamp expression."""
    return _calendar_part("month", value, _date_or_timestamp_argument)


def dayofmonth(value: object) -> Expression:
    """Extract the day of month from a Date or Timestamp expression."""
    return _calendar_part("dayofmonth", value, _date_or_timestamp_argument)


def hour(value: object) -> Expression:
    """Extract the hour from a Timestamp expression."""
    return _calendar_part("hour", value, _timestamp_argument)


def minute(value: object) -> Expression:
    """Extract the minute from a Timestamp expression."""
    return _calendar_part("minute", value, _timestamp_argument)


def second(value: object) -> Expression:
    """Extract the second from a Timestamp expression."""
    return _calendar_part("second", value, _timestamp_argument)


def to_date(value: object, *, format: str | None = None) -> Expression:
    """Convert a String, Date, or Timestamp expression to Date."""
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
    """Convert a String, Date, or Timestamp expression to Timestamp."""
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
    """Return the absolute value of a numeric expression."""
    argument = _numeric_argument(value, "abs(...)")
    return Expression(
        kind="call", type=argument.type, nullable=argument.nullable, data={"function": "abs"}, args=(argument,)
    )


def round(value: object, *, scale: int = 0) -> Expression:
    """Round a numeric expression with Spark ``round`` semantics."""
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
    """Round a numeric expression with Spark banker's rounding."""
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
    """Return the ceiling of a numeric expression."""
    argument = _numeric_argument(value, "ceil(...)")
    return Expression(
        kind="call",
        type=_ceiling_type(argument.type),
        nullable=argument.nullable,
        data={"function": "ceil"},
        args=(argument,),
    )


def floor(value: object) -> Expression:
    """Return the floor of a numeric expression."""
    argument = _numeric_argument(value, "floor(...)")
    return Expression(
        kind="call",
        type=_ceiling_type(argument.type),
        nullable=argument.nullable,
        data={"function": "floor"},
        args=(argument,),
    )


def sqrt(value: object) -> Expression:
    """Return the square root of a numeric expression."""
    return _double_numeric_call("sqrt", value)


def pow(value: object, exponent: object) -> Expression:
    """Raise a numeric expression to a numeric exponent."""
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
    """Return the natural logarithm or a logarithm with a literal base."""
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
    """Return ``e`` raised to a numeric expression."""
    return _double_numeric_call("exp", value)


def signum(value: object) -> Expression:
    """Return the sign of a numeric expression."""
    return _double_numeric_call("signum", value)


def isnull(value: object) -> Expression:
    """Return whether an expression is null."""
    return literal(value).is_null()


def isnotnull(value: object) -> Expression:
    """Return whether an expression is not null."""
    return literal(value).is_not_null()


def isnan(value: object) -> Expression:
    """Return whether a Float or Double expression is NaN."""
    argument = literal(value)
    if not isinstance(argument.type, (FloatType, DoubleType)):
        raise TypeError("isnan(...) requires a Float or Double Structure expression")
    return Expression(kind="is_nan", type=BooleanType(), nullable=False, args=(argument,))


def to_decimal(value: object, *, precision: int, scale: int) -> Expression:
    """Convert a compatible scalar expression to nullable Decimal."""
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
    """Return the first non-null value using Structure common-type rules.

    Args:
        *values: Compatible expressions or literals.

    Returns:
        A symbolic expression with the common Structure type.

    Example:
        display_name = coalesce(customer.nickname, customer.full_name, "unknown")
    """
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
    """Return ``fallback`` when ``value`` is null, like Spark ``nvl``."""
    return _null_fallback("nvl", value, fallback)


def ifnull(value: object, fallback: object) -> Expression:
    """Return ``fallback`` when ``value`` is null, like Spark ``ifnull``."""
    return _null_fallback("ifnull", value, fallback)


def nvl2(value: object, when_not_null: object, when_null: object) -> Expression:
    """Choose between two values based on whether an expression is null."""
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
    """Return zero for null numeric values."""
    argument = _numeric_argument(value, "zeroifnull(...)")
    if argument.type is None:
        raise AssertionError("numeric argument validation must reject untyped expressions")
    return Expression(
        kind="call", type=argument.type, nullable=False, data={"function": "zeroifnull"}, args=(argument,)
    )


def nullif(value: object, other: object) -> Expression:
    """Return null when two compatible expressions are equal."""
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
    """Return fallback when a Float or Double expression is NaN."""
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
    """Compare two event-time expressions with an inclusive interval bound.

    Args:
        left: Timestamp expression from one side of the comparison.
        right: Timestamp expression from the other side.
        upper: Non-negative fixed Spark interval upper bound.
        lower: Non-negative fixed Spark interval lower bound.

    Returns:
        A boolean expression suitable for streaming joins.

    Example:
        on_time = event_time_between(order.created_at, event.created_at, upper="5 minutes")
    """
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
    """Start a Spark ``when(...).otherwise(...)`` conditional expression.

    Args:
        condition: Boolean Structure expression.
        value: Expression or literal returned when ``condition`` is true.

    Returns:
        A builder that must be completed with ``otherwise(...)``.

    Example:
        tier = when(order.total >= 100, "premium").otherwise("standard")
    """
    predicate = literal(condition)
    if not isinstance(predicate.type, BooleanType):
        raise TypeError("when(...) requires a boolean Structure expression as its condition")
    return WhenBuilder(condition=predicate, value=literal(value))


@dataclass(frozen=True)
class WhenBuilder:
    """Intermediate conditional expression that requires ``otherwise(...)``."""

    condition: Expression
    value: Expression

    def otherwise(self, fallback: object) -> Expression:
        """Complete the conditional expression with the fallback branch."""
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


def _binary_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, BinaryType):
        raise TypeError(f"{call} requires a Binary Structure expression")
    return argument


def _struct_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, StructType):
        raise TypeError(f"{call} requires a Struct Structure expression")
    return argument


def _schema_argument(value: object, call: str):
    from structure.dsl import Schema

    if not isinstance(value, type) or not issubclass(value, Schema):
        raise TypeError(f"{call} as_= must be a Schema class")
    return value


def _parser_schema_argument(value: object, call: str):
    schema = _schema_argument(value, call)
    _parser_nullable_schema(schema, call, path=schema.__name__)
    return schema


def _parser_nullable_schema(schema: Any, call: str, *, path: str) -> None:
    for field in schema._structure_fields.values():
        field_path = f"{path}.{field.name}"
        if not field.nullable:
            raise TypeError(f"{call} as_= schema field {field_path} must be nullable for permissive parsing")
        if isinstance(field.type, StructType):
            _parser_nullable_schema(field.type.schema, call, path=field_path)


def _json_options(value: object) -> JsonOptions:
    if not isinstance(value, JsonOptions):
        raise TypeError("JSON conversion options must be a JsonOptions value")
    return value


def _csv_options(value: object) -> CsvOptions:
    if not isinstance(value, CsvOptions):
        raise TypeError("CSV conversion options must be a CsvOptions value")
    return value


def _spark_options(value: object, *, writer: bool, keys: Mapping[str, str]) -> dict[str, str]:
    options: dict[str, str] = {}
    for field, spark_name in keys.items():
        if writer and field == "mode":
            continue
        option = getattr(value, field)
        if option is None:
            continue
        if not isinstance(option, str):
            raise TypeError(f"{value.__class__.__name__}.{field} must be a string or None")
        if field == "mode":
            if option != "PERMISSIVE":
                raise TypeError(f'{value.__class__.__name__}.mode must be "PERMISSIVE"')
        elif option == "" and field != "null_value":
            raise TypeError(f"{value.__class__.__name__}.{field} must be a non-empty string or None")
        options[spark_name] = option
    return options


def _charset(value: object, call: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"{call} charset must be a non-empty string literal")
    return value


def _concat_ws_argument(value: object) -> Expression:
    argument = literal(value)
    if isinstance(argument.type, StringType):
        return argument
    if isinstance(argument.type, ArrayType) and isinstance(argument.type.element, StringType):
        return argument
    raise TypeError("concat_ws(...) requires a String or array<string> Structure expression")


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
        (
            BinaryType,
            BooleanType,
            StringType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            DecimalType,
            DateType,
            TimestampType,
        ),
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
