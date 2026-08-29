"""PySpark-compatible scalar expression helpers.

The functions in this module mirror common ``pyspark.sql.functions`` behavior
while returning Structure :class:`Expression` objects instead of Spark
``Column`` objects.  They validate types at authoring time so generated Spark
code is predictable and failures point to the DSL call that caused them.
"""

from __future__ import annotations

import builtins
import json
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
    VariantType,
)

__all__ = [
    "abs", "base64", "bin", "bround", "ceil", "coalesce", "concat_ws", "conv", "date_add", "date_sub", "date_trunc", "datediff",
    "dayofmonth", "event_time_between", "exp", "floor", "from_csv", "from_json", "hash", "hour", "ifnull", "initcap",
    "instr", "isnan", "isnotnull", "isnull", "CsvOptions", "JsonOptions", "length", "levenshtein", "literal", "log",
    "lower", "lpad", "ltrim", "md5",
    "minute", "month", "nanvl", "nullif", "nvl", "nvl2", "pow", "regexp_extract", "regexp_replace", "repeat", "replace", "reverse",
    "round", "rpad", "rtrim", "sha1", "sha2", "second", "signum", "split", "sqrt", "substring", "to_csv", "to_date",
    "to_decimal", "to_json", "to_timestamp", "translate", "trim", "trunc", "unbase64", "decode", "encode", "hex", "unhex", "upper", "ascii", "char_length", "left", "locate", "octet_length", "right", "substring_index",
    "when", "width_bucket", "xxhash64", "year", "zeroifnull", "acos", "acosh", "asin", "asinh", "atan", "atan2", "atanh", "cbrt", "cos", "cosh", "cot", "csc", "degrees", "e", "expm1", "factorial", "greatest", "hypot", "least", "ln", "log10", "log1p", "log2", "pmod", "pi", "radians", "rint", "sec", "sign", "sin", "sinh", "tan", "tanh", "add_months", "next_day", "rand", "is_valid_variant", "is_variant_null", "parse_json",
    "schema_of_variant", "to_variant_object", "try_parse_json", "try_variant_get", "variant_get", "variant_literal",
    "variant_array_append", "try_variant_array_append", "variant_insert", "try_variant_insert", "variant_set",
    "try_variant_set", "variant_delete",
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


def _reject_non_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant {value!r}")


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


def parse_json(value: object) -> Expression:
    """Parse JSON text into a Variant value, failing for invalid JSON."""
    argument = _string_argument(value, "parse_json(...)")
    return _variant_call("parse_json", argument, nullable=argument.nullable)


def variant_literal(json_text: str) -> Expression:
    """Construct a Variant from compile-time JSON text.

    The text is validated while authoring the transform and lowered through
    Spark's public ``parse_json`` function.  This keeps generated and online
    execution independent of PySpark's Python ``VariantVal`` representation.
    """
    if not isinstance(json_text, str) or not json_text.strip():
        raise TypeError("variant_literal(...) requires non-empty JSON text")
    try:
        json.loads(json_text, parse_constant=_reject_non_json_constant)
    except (TypeError, ValueError) as error:
        raise ValueError("variant_literal(...) requires valid JSON text") from error
    return parse_json(json_text)


def try_parse_json(value: object) -> Expression:
    """Parse JSON text into a Variant value, returning null for invalid JSON."""
    argument = _string_argument(value, "try_parse_json(...)")
    return _variant_call("try_parse_json", argument, nullable=True)


def variant_get(value: object, path: str, *, as_type: StructureType) -> Expression:
    """Extract and cast a literal Variant path, failing when Spark cannot cast it."""
    return _variant_get("variant_get", value, path, as_type)


def try_variant_get(value: object, path: str, *, as_type: StructureType) -> Expression:
    """Extract and cast a literal Variant path, returning null when it is absent or incompatible."""
    return _variant_get("try_variant_get", value, path, as_type)


def variant_array_append(value: object, path: str, element: object) -> Expression:
    """Append a value to an array inside a Variant, failing on a type mismatch."""
    return _variant_mutation("variant_array_append", value, path, element)


def try_variant_array_append(value: object, path: str, element: object) -> Expression:
    """Append a value to an array inside a Variant, returning null on failure."""
    return _variant_mutation("try_variant_array_append", value, path, element)


def variant_insert(value: object, path: str, element: object) -> Expression:
    """Insert a value into an object or array inside a Variant."""
    return _variant_mutation("variant_insert", value, path, element)


def try_variant_insert(value: object, path: str, element: object) -> Expression:
    """Insert a value into a Variant, returning null when insertion fails."""
    return _variant_mutation("try_variant_insert", value, path, element)


def variant_set(value: object, path: str, element: object, *, create_if_missing: bool = True) -> Expression:
    """Set or upsert a value at a JSON path inside a Variant."""
    if not isinstance(create_if_missing, bool):
        raise TypeError("variant_set(...) create_if_missing must be a Boolean literal")
    return _variant_mutation(
        "variant_set", value, path, element, data={"create_if_missing": create_if_missing}
    )


def try_variant_set(value: object, path: str, element: object, *, create_if_missing: bool = True) -> Expression:
    """Set or upsert a Variant value, returning null when the operation fails."""
    if not isinstance(create_if_missing, bool):
        raise TypeError("try_variant_set(...) create_if_missing must be a Boolean literal")
    return _variant_mutation(
        "try_variant_set", value, path, element, data={"create_if_missing": create_if_missing}
    )


def variant_delete(value: object, *paths: str) -> Expression:
    """Delete one or more literal JSON paths from a Variant."""
    argument = _variant_argument(value, "variant_delete(...)")
    if not paths:
        raise TypeError("variant_delete(...) requires at least one path")
    normalized = tuple(_variant_path(path, "variant_delete(...)", root_allowed=False) for path in paths)
    return _variant_call(
        "variant_delete",
        argument,
        nullable=argument.nullable,
        data={"paths": normalized},
    )


def to_variant_object(value: object) -> Expression:
    """Convert an Array, Map, or Struct expression into a Variant value.

    Spark Variant objects permit map keys only when they are Strings. Structure
    checks that invariant across nested Array, Map, and Struct declarations.
    """
    argument = literal(value)
    if not isinstance(argument.type, (ArrayType, MapType, StructType)):
        raise TypeError("to_variant_object(...) requires an Array, Map, or Struct Structure expression")
    _variant_compatible(argument.type, "to_variant_object(...)")
    return _variant_call("to_variant_object", argument, nullable=argument.nullable)


def is_variant_null(value: object) -> Expression:
    """Return whether a Variant value is Spark's JSON ``null`` rather than SQL null."""
    argument = _variant_argument(value, "is_variant_null(...)")
    return _variant_call("is_variant_null", argument, type=BooleanType(), nullable=False)


def is_valid_variant(value: object) -> Expression:
    """Return whether a Variant value is structurally valid."""
    argument = _variant_argument(value, "is_valid_variant(...)")
    return Expression(
        kind="call",
        type=BooleanType(),
        nullable=argument.nullable,
        data={
            "function": "is_valid_variant",
            "capability_group": "expression",
            "capability_name": "is_valid_variant",
        },
        args=(argument,),
    )


def schema_of_variant(value: object) -> Expression:
    """Return the SQL-format schema of one Variant value."""
    argument = _variant_argument(value, "schema_of_variant(...)")
    return _variant_call("schema_of_variant", argument, type=StringType(), nullable=argument.nullable)


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


def lpad(value: object, *, length: int, pad: str = " ") -> Expression:
    """Left-pad a string expression to a literal character length."""
    argument = _string_argument(value, "lpad(...)")
    _padding_length(length, "lpad(...)")
    _padding_string(pad, "lpad(...)")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "lpad", "length": length, "pad": pad},
        args=(argument,),
    )


def rpad(value: object, *, length: int, pad: str = " ") -> Expression:
    """Right-pad a string expression to a literal character length."""
    argument = _string_argument(value, "rpad(...)")
    _padding_length(length, "rpad(...)")
    _padding_string(pad, "rpad(...)")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "rpad", "length": length, "pad": pad},
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


def ascii(value: object) -> Expression:
    """Return the numeric value of the first character in a string."""
    argument = _string_argument(value, "ascii(...)")
    return Expression(kind="call", type=IntegerType(), nullable=argument.nullable, data={"function": "ascii"}, args=(argument,))


def char_length(value: object) -> Expression:
    """Return the character length of a string expression."""
    argument = _string_argument(value, "char_length(...)")
    return Expression(
        kind="call", type=IntegerType(), nullable=argument.nullable, data={"function": "char_length"}, args=(argument,)
    )


def left(value: object, *, length: int) -> Expression:
    """Return the leftmost literal number of characters from a string."""
    argument = _string_argument(value, "left(...)")
    _padding_length(length, "left(...)")
    return Expression(
        kind="call", type=StringType(), nullable=argument.nullable, data={"function": "left", "length": length}, args=(argument,)
    )


def right(value: object, *, length: int) -> Expression:
    """Return the rightmost literal number of characters from a string."""
    argument = _string_argument(value, "right(...)")
    _padding_length(length, "right(...)")
    return Expression(
        kind="call", type=StringType(), nullable=argument.nullable, data={"function": "right", "length": length}, args=(argument,)
    )


def locate(value: object, *, substring: str, position: int = 1) -> Expression:
    """Return the one-based position of a literal substring."""
    argument = _string_argument(value, "locate(...)")
    _string_literal(substring, "locate(...)", "substring")
    if isinstance(position, bool) or not isinstance(position, int) or position < 1:
        raise TypeError("locate(...) position must be a positive integer literal")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=argument.nullable,
        data={"function": "locate", "substring": substring, "position": position},
        args=(argument,),
    )


def octet_length(value: object) -> Expression:
    """Return the UTF-8 byte length of a String or Binary expression."""
    argument = _string_or_binary_argument(value, "octet_length(...)")
    return Expression(
        kind="call", type=IntegerType(), nullable=argument.nullable, data={"function": "octet_length"}, args=(argument,)
    )


def repeat(value: object, *, count: int) -> Expression:
    """Repeat a string a non-negative literal number of times."""
    argument = _string_argument(value, "repeat(...)")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise TypeError("repeat(...) count must be a non-negative integer literal")
    return Expression(
        kind="call", type=StringType(), nullable=argument.nullable, data={"function": "repeat", "count": count}, args=(argument,)
    )


def replace(value: object, *, search: str, replacement: str) -> Expression:
    """Replace literal occurrences in a string expression."""
    argument = _string_argument(value, "replace(...)")
    _string_literal(search, "replace(...)", "search")
    _string_literal(replacement, "replace(...)", "replacement")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "replace", "search": search, "replacement": replacement},
        args=(argument,),
    )


def substring_index(value: object, *, delimiter: str, count: int) -> Expression:
    """Return the substring before a literal delimiter occurrence count."""
    argument = _string_argument(value, "substring_index(...)")
    _string_literal(delimiter, "substring_index(...)", "delimiter")
    if isinstance(count, bool) or not isinstance(count, int):
        raise TypeError("substring_index(...) count must be an integer literal")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "substring_index", "delimiter": delimiter, "count": count},
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


def add_months(value: object, *, months: object) -> Expression:
    """Add whole calendar months and return a Date expression."""
    argument = _date_or_timestamp_argument(value, "add_months(...)")
    if isinstance(months, bool):
        raise TypeError("add_months(...) months must be an integer or integral Structure expression")
    if isinstance(months, int):
        return Expression(
            kind="call",
            type=DateType(),
            nullable=argument.nullable,
            data={"function": "add_months", "months": months},
            args=(argument,),
        )
    month_count = literal(months)
    if not isinstance(month_count.type, (IntegerType, LongType)):
        raise TypeError("add_months(...) months must be an integer or integral Structure expression")
    return Expression(
        kind="call",
        type=DateType(),
        nullable=argument.nullable or month_count.nullable,
        data={"function": "add_months"},
        args=(argument, month_count),
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


def next_day(value: object, *, day_of_week: str) -> Expression:
    """Return the first named weekday after a Date or Timestamp expression."""
    argument = _date_or_timestamp_argument(value, "next_day(...)")
    day = _weekday_literal(day_of_week, "next_day(...)")
    return Expression(
        kind="call",
        type=DateType(),
        nullable=argument.nullable,
        data={"function": "next_day", "day_of_week": day},
        args=(argument,),
    )


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


def bin(value: object) -> Expression:
    """Return the binary representation of an integral expression."""
    argument = _integral_argument(value, "bin(...)")
    return Expression(
        kind="call", type=StringType(), nullable=argument.nullable, data={"function": "bin"}, args=(argument,)
    )


def conv(value: object, *, from_base: int, to_base: int) -> Expression:
    """Convert a String number between validated literal bases."""
    argument = _string_argument(value, "conv(...)")
    _number_base(from_base, "conv(...) from_base")
    _number_base(to_base, "conv(...) to_base")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=True,
        data={"function": "conv", "from_base": from_base, "to_base": to_base},
        args=(argument,),
    )


def e() -> Expression:
    """Return Euler's number as a non-null Double expression."""
    return _constant_double_call("e")


def acos(value: object) -> Expression:
    """Return the arc cosine of a numeric expression in radians."""
    return _double_numeric_call("acos", value)


def acosh(value: object) -> Expression:
    """Return the inverse hyperbolic cosine of a numeric expression."""
    return _double_numeric_call("acosh", value)


def asin(value: object) -> Expression:
    """Return the arc sine of a numeric expression in radians."""
    return _double_numeric_call("asin", value)


def asinh(value: object) -> Expression:
    """Return the inverse hyperbolic sine of a numeric expression."""
    return _double_numeric_call("asinh", value)


def atan(value: object) -> Expression:
    """Return the arc tangent of a numeric expression in radians."""
    return _double_numeric_call("atan", value)


def atan2(y: object, x: object) -> Expression:
    """Return the two-argument arc tangent in radians."""
    return _double_numeric_binary_call("atan2", y, x)


def atanh(value: object) -> Expression:
    """Return the inverse hyperbolic tangent of a numeric expression."""
    return _double_numeric_call("atanh", value)


def cbrt(value: object) -> Expression:
    """Return the cube root of a numeric expression."""
    return _double_numeric_call("cbrt", value)


def cos(value: object) -> Expression:
    """Return the cosine of a numeric expression in radians."""
    return _double_numeric_call("cos", value)


def cosh(value: object) -> Expression:
    """Return the hyperbolic cosine of a numeric expression."""
    return _double_numeric_call("cosh", value)


def cot(value: object) -> Expression:
    """Return the cotangent of a numeric expression in radians."""
    return _double_numeric_call("cot", value)


def csc(value: object) -> Expression:
    """Return the cosecant of a numeric expression in radians."""
    return _double_numeric_call("csc", value)


def hypot(left: object, right: object) -> Expression:
    """Return the hypotenuse of two numeric expressions."""
    left_argument = _numeric_argument(left, "hypot(...)")
    right_argument = _numeric_argument(right, "hypot(...)")
    return Expression(
        kind="call",
        type=DoubleType(),
        nullable=left_argument.nullable or right_argument.nullable,
        data={"function": "hypot"},
        args=(left_argument, right_argument),
    )


def hex(value: object) -> Expression:
    """Return the hexadecimal representation of an integral or binary expression."""
    argument = literal(value)
    if not isinstance(argument.type, (BinaryType, IntegerType, LongType)):
        raise TypeError("hex(...) requires a Binary, Integer, or Long Structure expression")
    return Expression(
        kind="call", type=StringType(), nullable=argument.nullable, data={"function": "hex"}, args=(argument,)
    )


def least(*values: object) -> Expression:
    """Return the least value among at least two compatible expressions."""
    return _common_value_call("least", values)


def rand(*, seed: int | None = None, reproducible: bool = True) -> Expression:
    """Generate a non-null uniform random Double expression.

    ``reproducible=True`` requires a literal integer seed as an authoring
    policy. A seed makes the expression auditable but does not promise identical
    values across repartitioning, retries, Spark versions, or query restarts.
    Set ``reproducible=False`` to explicitly allow an omitted seed.
    """
    if not isinstance(reproducible, bool):
        raise TypeError("rand(...) reproducible must be a Boolean")
    if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
        raise TypeError("rand(...) seed must be an integer literal or None")
    if reproducible and seed is None:
        raise TypeError("rand(...) seed is required unless reproducible=False")
    return Expression(
        kind="call",
        type=DoubleType(),
        nullable=False,
        data={
            "function": "rand",
            "seed": seed,
            "reproducible": reproducible,
            "nondeterministic": True,
        },
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


def pi() -> Expression:
    """Return pi as a non-null Double expression."""
    return _constant_double_call("pi")


def unhex(value: object) -> Expression:
    """Decode a hexadecimal String expression into nullable Binary."""
    argument = _string_argument(value, "unhex(...)")
    return Expression(kind="call", type=BinaryType(), nullable=True, data={"function": "unhex"}, args=(argument,))


def width_bucket(value: object, minimum: object, maximum: object, *, num_buckets: int) -> Expression:
    """Return a nullable integer histogram bucket for compatible numeric values."""
    arguments = tuple(_numeric_argument(item, "width_bucket(...)") for item in (value, minimum, maximum))
    if any(argument.type is None for argument in arguments):
        raise AssertionError("numeric argument validation must reject untyped expressions")
    _positive_integer_literal(num_buckets, "width_bucket(...) num_buckets")
    _common_numeric_type("width_bucket(...)", tuple(argument.type for argument in arguments if argument.type is not None))
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=True,
        data={"function": "width_bucket", "num_buckets": num_buckets},
        args=arguments,
    )


def pmod(left: object, right: object) -> Expression:
    """Return the positive modulo of two numeric expressions."""
    return _numeric_binary_common_call("pmod", left, right)


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


def expm1(value: object) -> Expression:
    """Return ``e`` raised to a numeric expression minus one."""
    return _double_numeric_call("expm1", value)


def factorial(value: object) -> Expression:
    """Return the factorial of an integral expression as a nullable Long."""
    argument = _integral_argument(value, "factorial(...)")
    return Expression(
        kind="call", type=LongType(), nullable=argument.nullable, data={"function": "factorial"}, args=(argument,)
    )


def greatest(*values: object) -> Expression:
    """Return the greatest value among at least two compatible expressions."""
    return _common_value_call("greatest", values)


def degrees(value: object) -> Expression:
    """Convert a numeric angle from radians to degrees."""
    return _double_numeric_call("degrees", value)


def ln(value: object) -> Expression:
    """Return the natural logarithm of a numeric expression."""
    return _double_numeric_call("ln", value)


def log10(value: object) -> Expression:
    """Return the base-ten logarithm of a numeric expression."""
    return _double_numeric_call("log10", value)


def log1p(value: object) -> Expression:
    """Return the natural logarithm of one plus a numeric expression."""
    return _double_numeric_call("log1p", value)


def log2(value: object) -> Expression:
    """Return the base-two logarithm of a numeric expression."""
    return _double_numeric_call("log2", value)


def radians(value: object) -> Expression:
    """Convert a numeric angle from degrees to radians."""
    return _double_numeric_call("radians", value)


def rint(value: object) -> Expression:
    """Round a numeric expression to the nearest integer-valued Double."""
    return _double_numeric_call("rint", value)


def sec(value: object) -> Expression:
    """Return the secant of a numeric expression in radians."""
    return _double_numeric_call("sec", value)


def sign(value: object) -> Expression:
    """Return the sign of a numeric expression."""
    return _double_numeric_call("sign", value)


def sin(value: object) -> Expression:
    """Return the sine of a numeric expression in radians."""
    return _double_numeric_call("sin", value)


def sinh(value: object) -> Expression:
    """Return the hyperbolic sine of a numeric expression."""
    return _double_numeric_call("sinh", value)


def signum(value: object) -> Expression:
    """Return the sign of a numeric expression."""
    return _double_numeric_call("signum", value)


def tan(value: object) -> Expression:
    """Return the tangent of a numeric expression in radians."""
    return _double_numeric_call("tan", value)


def tanh(value: object) -> Expression:
    """Return the hyperbolic tangent of a numeric expression."""
    return _double_numeric_call("tanh", value)


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


def _string_or_binary_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, (StringType, BinaryType)):
        raise TypeError(f"{call} requires a String or Binary Structure expression")
    return argument


def _binary_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, BinaryType):
        raise TypeError(f"{call} requires a Binary Structure expression")
    return argument


def _variant_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, VariantType):
        raise TypeError(f"{call} requires a Variant Structure expression")
    return argument


def _variant_call(
    function: str,
    argument: Expression,
    *,
    type: StructureType | None = None,
    nullable: bool,
    data: Mapping[str, object] | None = None,
) -> Expression:
    return Expression(
        kind="call",
        type=VariantType() if type is None else type,
        nullable=nullable,
        data={
            "function": function,
            "capability_group": "expression",
            "capability_name": "variant",
            **(data or {}),
        },
        args=(argument,),
    )


def _variant_get(function: str, value: object, path: str, as_type: StructureType) -> Expression:
    argument = _variant_argument(value, f"{function}(...)")
    if not isinstance(path, str) or not path:
        raise TypeError(f"{function}(...) path must be a non-empty string literal")
    if not path.startswith("$"):
        raise ValueError(f"{function}(...) path must start with '$'")
    if not isinstance(as_type, StructureType):
        raise TypeError(f"{function}(...) as_type must be a Structure type such as types.string()")
    return _variant_call(
        function,
        argument,
        type=as_type,
        nullable=True,
        data={"path": path, "target_type": _variant_ddl(as_type)},
    )


def _variant_mutation(
    function: str,
    value: object,
    path: str,
    element: object,
    *,
    data: Mapping[str, object] | None = None,
) -> Expression:
    argument = _variant_argument(value, f"{function}(...)")
    normalized_path = _variant_path(path, f"{function}(...)")
    replacement = literal(element)
    return Expression(
        kind="call",
        type=VariantType(),
        nullable=argument.nullable or replacement.nullable,
        data={
            "function": function,
            "capability_group": "expression",
            "capability_name": function,
            "path": normalized_path,
            **(data or {}),
        },
        args=(argument, replacement),
    )


def _variant_path(path: str, call: str, *, root_allowed: bool = True) -> str:
    if not isinstance(path, str) or not path:
        raise TypeError(f"{call} path must be a non-empty string literal")
    if not path.startswith("$"):
        raise ValueError(f"{call} path must start with '$'")
    if not root_allowed and path == "$":
        raise ValueError(f"{call} path must identify a field or array element")
    return path


def _variant_compatible(type: StructureType, call: str) -> None:
    if isinstance(type, ArrayType):
        _variant_compatible(type.element, call)
    elif isinstance(type, MapType):
        if not isinstance(type.key, StringType):
            raise TypeError(f"{call} requires String Map keys at every nesting level")
        _variant_compatible(type.value, call)
    elif isinstance(type, StructType):
        for field in type.schema._structure_fields.values():
            _variant_compatible(field.type, call)


def _variant_ddl(type: StructureType) -> str:
    scalar = {"integer": "int", "long": "bigint"}.get(type.name, type.name)
    if scalar not in {"array", "map", "struct"}:
        if isinstance(type, DecimalType):
            return f"decimal({type.precision},{type.scale})"
        return scalar
    if isinstance(type, ArrayType):
        return f"array<{_variant_ddl(type.element)}>"
    if isinstance(type, MapType):
        return f"map<{_variant_ddl(type.key)},{_variant_ddl(type.value)}>"
    if isinstance(type, StructType):
        fields = ",".join(f"{field.column}:{_variant_ddl(field.type)}" for field in type.schema._structure_fields.values())
        return f"struct<{fields}>"
    raise TypeError(f"Unsupported Variant extraction type: {type!r}")


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


def _padding_length(value: object, call: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TypeError(f"{call} length must be a non-negative integer literal")


def _padding_string(value: object, call: str) -> None:
    if not isinstance(value, str) or not value:
        raise TypeError(f"{call} pad must be a non-empty string literal")


def _number_base(value: object, parameter: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 2 <= builtins.abs(value) <= 36:
        raise TypeError(f"{parameter} must be an integer literal from -36 through -2 or 2 through 36")


def _positive_integer_literal(value: object, parameter: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TypeError(f"{parameter} must be a positive integer literal")


def _weekday_literal(value: object, call: str) -> str:
    if not isinstance(value, str) or value.lower() not in {
        "mon", "monday", "tue", "tuesday", "wed", "wednesday", "thu", "thursday", "fri", "friday",
        "sat", "saturday", "sun", "sunday",
    }:
        raise TypeError(f"{call} day_of_week must name a weekday such as 'Mon' or 'Monday'")
    return value


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


def _double_numeric_binary_call(function: str, left: object, right: object) -> Expression:
    left_argument = _numeric_argument(left, f"{function}(...)")
    right_argument = _numeric_argument(right, f"{function}(...)")
    return Expression(
        kind="call",
        type=DoubleType(),
        nullable=left_argument.nullable or right_argument.nullable,
        data={"function": function},
        args=(left_argument, right_argument),
    )


def _constant_double_call(function: str) -> Expression:
    return Expression(kind="call", type=DoubleType(), nullable=False, data={"function": function})


def _integral_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, (IntegerType, LongType)):
        raise TypeError(f"{call} requires an integer or long Structure expression")
    return argument


def _numeric_binary_common_call(function: str, left: object, right: object) -> Expression:
    left_argument = _numeric_argument(left, f"{function}(...)")
    right_argument = _numeric_argument(right, f"{function}(...)")
    if left_argument.type is None or right_argument.type is None:
        raise AssertionError("numeric argument validation must reject untyped expressions")
    result_type = _common_numeric_type(function, (left_argument.type, right_argument.type))
    return Expression(
        kind="call",
        type=result_type,
        nullable=left_argument.nullable or right_argument.nullable,
        data={"function": function},
        args=(left_argument, right_argument),
    )


def _common_value_call(function: str, values: tuple[object, ...]) -> Expression:
    if len(values) < 2:
        raise TypeError(f"{function}(...) requires at least two values")
    arguments = tuple(literal(value) for value in values)
    result_type = _common_type(f"{function}(...)", arguments)
    if result_type is None:
        raise TypeError(f"{function}(...) requires at least one typed Structure expression")
    return Expression(
        kind="call",
        type=result_type,
        nullable=all(argument.nullable for argument in arguments),
        data={"function": function},
        args=arguments,
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
