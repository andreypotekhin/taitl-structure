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
from typing import TYPE_CHECKING, Any, Mapping, overload

from structure.dsl import FieldDeclaration
from structure.plugin.api.v1.model.CompilationSettings import current_compilation_settings
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.types import (
    ArrayType,
    BinaryType,
    BitmapType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    HllSketchType,
    IntegerType,
    IntervalType,
    LongType,
    MapType,
    StringType,
    StructType,
    StructureType,
    TimestampNTZType,
    TimestampType,
    VariantType,
)

if TYPE_CHECKING:
    from structure.plugin.pyspark.dsl.RowScope import RowScope

__all__ = [
    "abs", "assert_true", "base64", "bit_length", "bin", "bround", "ceil", "coalesce", "concat_ws", "conv", "date_add", "date_sub", "date_trunc", "datediff",
    "dayofmonth", "dayofweek", "dayofyear", "event_time_between", "exp", "floor", "from_csv", "from_json", "hash", "hour", "ifnull", "initcap",
    "instr", "isnan", "isnotnull", "isnull", "CsvOptions", "JsonOptions", "length", "levenshtein", "literal", "log",
    "get_json_object", "json_array_length", "json_object_keys", "schema_of_csv", "schema_of_json",
    "lower", "lpad", "ltrim", "mask", "md5", "crc32", "elt", "format_string", "printf", "substr",
    "minute", "month", "nanvl", "nullif", "nvl", "nvl2", "pow", "regexp_extract", "regexp_replace", "repeat", "replace", "reverse",
    "round", "rpad", "rtrim", "sha1", "sha2", "second", "signum", "split", "sqrt", "substring", "to_csv", "to_date",
    "to_binary", "to_decimal", "to_json", "to_timestamp", "to_timestamp_ntz", "translate", "trim", "trunc", "unbase64", "decode", "encode", "try_to_binary", "from_unixtime", "unix_timestamp", "to_utc_timestamp", "from_utc_timestamp", "date_part", "datepart", "hex", "unhex", "upper", "ascii", "btrim", "char", "char_length", "date_format", "find_in_set", "format_number", "last_day", "left", "locate", "mask", "octet_length", "overlay", "position", "quarter", "right", "soundex", "split_part", "substring_index", "regexp_count", "regexp_extract_all", "regexp_instr", "regexp_substr", "weekofyear", "bit_count", "bit_get", "getbit",
    "when", "width_bucket", "xxhash64", "year", "zeroifnull", "acos", "acosh", "asin", "asinh", "atan", "atan2", "atanh", "cbrt", "cos", "cosh", "cot", "csc", "degrees", "e", "expm1", "factorial", "greatest", "hypot", "least", "ln", "log10", "log1p", "log2", "pmod", "pi", "radians", "rint", "sec", "sign", "sin", "sinh", "tan", "tanh", "ceiling", "negate", "negative", "positive", "power", "bitwise_not", "add_months", "months_between", "next_day", "rand", "randn", "equal_null", "like", "ilike", "regexp", "regexp_like", "rlike", "startswith", "endswith", "date_from_unix_date", "unix_date", "weekday", "shiftleft", "shiftright", "shiftrightunsigned", "is_valid_variant", "is_variant_null", "parse_json",
    "schema_of_variant", "to_variant_object", "try_parse_json", "try_variant_get", "variant_get", "variant_literal",
    "variant_array_append", "try_variant_array_append", "variant_insert", "try_variant_insert", "variant_set",
    "try_variant_set", "variant_delete",
    "raise_error", "current_date", "curdate", "current_timestamp", "now", "localtimestamp", "current_timezone",
    "aes_encrypt", "aes_decrypt", "try_aes_decrypt",
    "hll_sketch_estimate", "hll_union", "bitmap_bit_position", "bitmap_bucket_number", "bitmap_count",
    "url_encode", "url_decode", "try_url_decode", "make_date", "date_diff", "dateadd", "day",
    "convert_timezone", "make_timestamp", "try_to_timestamp", "to_timestamp_ltz", "to_unix_timestamp", "timestamp_seconds", "timestamp_millis",
    "timestamp_micros", "unix_seconds", "unix_millis", "unix_micros", "make_timestamp_ltz", "make_timestamp_ntz",
    "interval", "make_interval", "make_ym_interval", "make_dt_interval", "extract",
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
    from structure.dsl import VariableReference

    if isinstance(value, VariableReference):
        return variable_expression(value)

    if isinstance(value, Expression):
        return value

    if isinstance(value, FieldDeclaration):
        return value._structure_expression()

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


def variable_expression(reference) -> Expression:
    """Create a typed PySpark expression for one invocation-bound scalar."""
    python_type = reference.python_type
    spark_type: StructureType
    if python_type is bool:
        spark_type = BooleanType()
    elif python_type is int:
        spark_type = LongType()
    elif python_type is float:
        spark_type = DoubleType()
    elif python_type is str:
        spark_type = StringType()
    elif python_type is bytes:
        spark_type = BinaryType()
    elif python_type is Decimal:
        spark_type = DecimalType(reference.precision, reference.scale)
    elif python_type is date:
        spark_type = DateType()
    elif python_type is datetime:
        spark_type = TimestampType()
    else:
        raise TypeError(f"Unsupported runtime variable type {python_type!r}")
    return Expression(
        kind="variable",
        type=spark_type,
        nullable=reference.nullable,
        data={"name": reference.name},
    )


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


def lcase(value: object) -> Expression:
    """Lowercase a string expression using PySpark's ``lcase`` spelling."""
    return _string_call("lcase", value)


def ltrim(value: object) -> Expression:
    """Trim leading whitespace from a string expression, like Spark ``ltrim``."""
    return _string_call("ltrim", value)


def rtrim(value: object) -> Expression:
    """Trim trailing whitespace from a string expression, like Spark ``rtrim``."""
    return _string_call("rtrim", value)


def trim(value: object) -> Expression:
    """Trim leading and trailing whitespace from a string expression."""
    return _string_call("trim", value)


def btrim(value: object, *, trim: object = " ") -> Expression:
    """Trim characters from both ends of a string expression.

    ``trim`` may be a compiler-visible String expression, matching PySpark's
    column-valued trim argument.
    """
    argument = _string_argument(value, "btrim(...)")
    trim_argument = _string_argument(trim, "btrim(...)")
    data: dict[str, object] = {"function": "btrim"}
    args = (argument, trim_argument)
    if trim_argument.kind == "literal":
        trim_value = (trim_argument.data or {}).get("value")
        assert isinstance(trim_value, str)
        data["trim"] = trim_value
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable or trim_argument.nullable,
        data=data,
        args=args,
    )


def upper(value: object) -> Expression:
    """Uppercase a string expression, like Spark ``upper``."""
    return _string_call("upper", value)


def ucase(value: object) -> Expression:
    """Uppercase a string expression using PySpark's ``ucase`` spelling."""
    return _string_call("ucase", value)


def url_encode(value: object) -> Expression:
    """Encode a String expression using Spark's URL form-encoding rules."""
    return _string_call("url_encode", value)


def url_decode(value: object) -> Expression:
    """Decode a URL-encoded String expression; malformed input follows Spark's strict behavior."""
    return _string_call("url_decode", value)


def try_url_decode(value: object) -> Expression:
    """Decode a URL-encoded String expression, returning null for malformed input on PySpark 4.0+."""
    return _string_call("try_url_decode", value)


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


def to_binary(value: object, *, format: str | None = None) -> Expression:
    """Convert text to binary using Spark's literal conversion formats.

    ``format`` accepts ``hex``, ``utf-8``, ``utf8``, or ``base64``. When it
    is omitted, Spark uses ``hex``. Invalid input raises during Spark
    evaluation, matching PySpark ``to_binary``.
    """
    argument = _string_argument(value, "to_binary(...)")
    format = _binary_format(format, "to_binary(...)")
    return Expression(
        kind="call",
        type=BinaryType(),
        nullable=argument.nullable,
        data={"function": "to_binary", **({"format": format} if format is not None else {})},
        args=(argument,),
    )


def try_to_binary(value: object, *, format: str | None = None) -> Expression:
    """Convert text to binary, returning null when Spark cannot convert it."""
    argument = _string_argument(value, "try_to_binary(...)")
    format = _binary_format(format, "try_to_binary(...)")
    return Expression(
        kind="call",
        type=BinaryType(),
        nullable=True,
        data={"function": "try_to_binary", **({"format": format} if format is not None else {})},
        args=(argument,),
    )


def _query_clock(function: str, type: StructureType) -> Expression:
    return Expression(
        kind="call",
        type=type,
        nullable=False,
        data={
            "function": function,
            "nondeterministic": True,
            "query_stable": True,
            "capability_group": "expression",
            "capability_name": "query_clock",
        },
    )


def current_date() -> Expression:
    """Return the query-start date, stable within one Spark query."""
    return _query_clock("current_date", DateType())


def curdate() -> Expression:
    """Alias for :func:`current_date`."""
    return _query_clock("curdate", DateType())


def current_timestamp() -> Expression:
    """Return the query-start timestamp, stable within one Spark query."""
    return _query_clock("current_timestamp", TimestampType())


def now() -> Expression:
    """Alias for :func:`current_timestamp`."""
    return _query_clock("now", TimestampType())


def localtimestamp() -> Expression:
    """Return Spark's query-start local timestamp."""
    return _query_clock("localtimestamp", TimestampNTZType())


def current_timezone() -> Expression:
    """Return the active Spark session timezone configuration."""
    return Expression(
        kind="call",
        type=StringType(),
        nullable=False,
        data={
            "function": "current_timezone",
            "capability_group": "expression",
            "capability_name": "query_clock",
        },
    )


def aes_encrypt(
    value: object,
    *,
    key: object,
    aad: object | None = None,
    iv: object | None = None,
) -> Expression:
    """Encrypt a String or Binary value with Spark's AES-GCM contract.

    ``key`` is deliberately symbolic: secrets must not be embedded as Python
    literals in generated source.  An explicit ``iv`` is accepted for
    interoperability but marks the expression with a caller-ownership warning.
    """
    return _aes_call("aes_encrypt", value, key=key, aad=aad, iv=iv, nullable=True)


def aes_decrypt(value: object, *, key: object, aad: object | None = None) -> Expression:
    """Decrypt a String or Binary value using Spark's strict AES-GCM helper."""
    return _aes_call("aes_decrypt", value, key=key, aad=aad, nullable=True)


def try_aes_decrypt(value: object, *, key: object, aad: object | None = None) -> Expression:
    """Decrypt AES-GCM data, returning null for invalid ciphertext or keys."""
    return _aes_call("try_aes_decrypt", value, key=key, aad=aad, nullable=True)


def _aes_call(
    function: str,
    value: object,
    *,
    key: object,
    aad: object | None = None,
    iv: object | None = None,
    nullable: bool,
) -> Expression:
    payload = _string_or_binary_argument(value, f"{function}(...)")
    if not isinstance(key, Expression):
        raise TypeError(f"{function}(...) key must be a symbolic String or Binary expression")
    key_expression = _string_or_binary_argument(key, f"{function}(...)")
    arguments = [payload, key_expression]
    if aad is not None:
        arguments.append(_string_or_binary_argument(aad, f"{function}(...)"))
    if iv is not None:
        if function != "aes_encrypt":
            raise TypeError("aes_decrypt(...) IVs are not part of the Structure contract")
        arguments.append(_binary_argument(iv, f"{function}(...)"))
    data: dict[str, object] = {
        "function": function,
        "capability_group": "expression",
        "capability_name": "aes_gcm",
    }
    if iv is not None:
        data["warnings"] = ("CRYPTO-W0801",)
        data["manual_iv"] = True
    return Expression(
        kind="call",
        type=BinaryType(),
        nullable=nullable or any(argument.nullable for argument in arguments),
        data=data,
        args=tuple(arguments),
    )


def hll_sketch_estimate(value: object) -> Expression:
    """Estimate the cardinality represented by an opaque HLL sketch."""
    argument = _sketch_argument(value, HllSketchType, "hll_sketch_estimate(...)")
    return Expression(
        kind="call", type=LongType(), nullable=argument.nullable,
        data={"function": "hll_sketch_estimate", "capability_group": "expression", "capability_name": "sketches"},
        args=(argument,),
    )


def hll_union(left: object, right: object, *, allow_different_lg_config_k: bool = False) -> Expression:
    """Union two HLL sketches, rejecting precision mismatches by default."""
    if not isinstance(allow_different_lg_config_k, bool):
        raise TypeError("hll_union(...) allow_different_lg_config_k must be a Boolean")
    first = _sketch_argument(left, HllSketchType, "hll_union(...)")
    second = _sketch_argument(right, HllSketchType, "hll_union(...)")
    first_type = first.type
    second_type = second.type
    assert isinstance(first_type, HllSketchType) and isinstance(second_type, HllSketchType)
    if first_type.lg_config_k != second_type.lg_config_k and not allow_different_lg_config_k:
        raise TypeError("hll_union(...) requires matching lg_config_k unless allow_different_lg_config_k=True")
    data: dict[str, object] = {
        "function": "hll_union",
        "allow_different_lg_config_k": allow_different_lg_config_k,
        "capability_group": "expression",
        "capability_name": "sketches",
    }
    if first_type.lg_config_k != second_type.lg_config_k:
        data["warnings"] = ("SKETCH-W0802",)
    return Expression(
        kind="call", type=first_type, nullable=first.nullable or second.nullable,
        data=data, args=(first, second),
    )


def bitmap_bit_position(value: object) -> Expression:
    argument = _integral_argument(value, "bitmap_bit_position(...)")
    return Expression(kind="call", type=LongType(), nullable=argument.nullable, data={"function": "bitmap_bit_position"}, args=(argument,))


def bitmap_bucket_number(value: object) -> Expression:
    argument = _integral_argument(value, "bitmap_bucket_number(...)")
    return Expression(kind="call", type=LongType(), nullable=argument.nullable, data={"function": "bitmap_bucket_number"}, args=(argument,))


def bitmap_count(value: object) -> Expression:
    argument = _sketch_argument(value, BitmapType, "bitmap_count(...)")
    return Expression(kind="call", type=LongType(), nullable=argument.nullable, data={"function": "bitmap_count"}, args=(argument,))


def _sketch_argument(value: object, expected: type[StructureType], call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, expected):
        raise TypeError(f"{call} requires an opaque {expected.__name__} Structure expression")
    return argument


def from_json(value: object, *, to: type, options: JsonOptions = JsonOptions()) -> Expression:
    """Parse JSON text into a declared Structure record, like PySpark ``from_json``.

    Args:
        value: String Structure expression or Python string literal containing JSON.
        to: ``Schema`` class that declares the parsed struct shape.
        options: Immutable JSON parser options.

    Returns:
        A nullable struct expression with the exact declared ``to`` schema.

    Example:
        payload = from_json(raw.payload_json, to=Payload)
    """
    argument = _string_argument(value, "from_json(...)")
    schema = _parser_schema_argument(to, "from_json(...)")
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


def from_csv(value: object, *, to: type, options: CsvOptions = CsvOptions()) -> Expression:
    """Parse CSV text into a declared Structure record, like PySpark ``from_csv``.

    Args:
        value: String Structure expression or Python string literal containing one CSV row.
        to: ``Schema`` class that declares the parsed struct shape.
        options: Immutable CSV parser options.

    Returns:
        A nullable struct expression with the exact declared ``to`` schema.

    Example:
        payload = from_csv(raw.payload_csv, to=Payload, options=CsvOptions(delimiter="|"))
    """
    argument = _string_argument(value, "from_csv(...)")
    schema = _parser_schema_argument(to, "from_csv(...)")
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


def get_json_object(value: object, path: str) -> Expression:
    """Extract a JSON value as nullable String using a literal JSON path."""
    argument = _string_argument(value, "get_json_object(...)")
    if not isinstance(path, str) or not path:
        raise TypeError("get_json_object(...) path must be a non-empty string literal")
    return Expression(
        kind="call", type=StringType(), nullable=True, data={"function": "get_json_object", "path": path}, args=(argument,)
    )


def json_array_length(value: object) -> Expression:
    """Return the nullable length of a JSON array string."""
    argument = _string_argument(value, "json_array_length(...)")
    return Expression(kind="call", type=IntegerType(), nullable=True, data={"function": "json_array_length"}, args=(argument,))


def json_object_keys(value: object) -> Expression:
    """Return nullable JSON object keys as an array of Strings."""
    argument = _string_argument(value, "json_object_keys(...)")
    return Expression(
        kind="call", type=ArrayType(StringType(), contains_null=False), nullable=True, data={"function": "json_object_keys"}, args=(argument,)
    )


def schema_of_json(value: object, *, options: JsonOptions = JsonOptions()) -> Expression:
    """Infer a Spark SQL schema string from a valid JSON literal."""
    if not isinstance(value, str) or not value:
        raise TypeError("schema_of_json(...) requires non-empty JSON text literal")
    try:
        json.loads(value, parse_constant=_reject_non_json_constant)
    except (TypeError, ValueError) as error:
        raise ValueError("schema_of_json(...) requires valid JSON text") from error
    if not isinstance(options, JsonOptions):
        raise TypeError("schema_of_json(...) options must be a JsonOptions value")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=False,
        data={"function": "schema_of_json", "options": options.spark_options()},
        args=(literal(value),),
    )


def schema_of_csv(value: object, *, options: CsvOptions = CsvOptions()) -> Expression:
    """Infer a Spark SQL schema string from a CSV text literal."""
    if not isinstance(value, str) or not value:
        raise TypeError("schema_of_csv(...) requires non-empty CSV text literal")
    if not isinstance(options, CsvOptions):
        raise TypeError("schema_of_csv(...) options must be a CsvOptions value")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=False,
        data={"function": "schema_of_csv", "options": options.spark_options()},
        args=(literal(value),),
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


def substring(value: object, *, start: object, length: object) -> Expression:
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
    return _substring_call("substring", value, start=start, length=length)


def substr(value: object, *, start: object, length: object) -> Expression:
    """Return a substring expression using Spark's ``substr`` spelling."""
    return _substring_call("substr", value, start=start, length=length)


def _column_substr(value: object, start: object, length: object) -> Expression:
    return _substring_call("substr", value, start=start, length=length, method=True)


def elt(index: object, *values: object) -> Expression:
    """Return the one-based selected value from compatible scalar expressions."""
    if not values:
        raise TypeError("elt(...) requires at least one value")
    position = _integral_argument(index, "elt(...)")
    arguments = tuple(_scalar_argument(value, "elt(...)") for value in values)
    result_type = _common_type("elt(...)", arguments)
    if result_type is None:
        raise TypeError("elt(...) requires at least one typed Structure expression")
    return Expression(
        kind="call",
        type=result_type,
        nullable=position.nullable or any(argument.nullable for argument in arguments),
        data={"function": "elt"},
        args=(position, *arguments),
    )


def format_string(format: str, *values: object) -> Expression:
    """Format scalar expressions with a literal Spark format string."""
    return _format_string_call("format_string", format, values)


def printf(format: str, *values: object) -> Expression:
    """Format scalar expressions using Spark's ``printf`` spelling."""
    return _format_string_call("printf", format, values)


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


def regexp_count(value: object, *, pattern: str) -> Expression:
    """Count non-overlapping matches of a literal regular expression."""
    argument = _string_argument(value, "regexp_count(...)")
    _string_literal(pattern, "regexp_count(...)", "pattern")
    return Expression(
        kind="call", type=IntegerType(), nullable=argument.nullable,
        data={"function": "regexp_count", "pattern": pattern}, args=(argument,)
    )


def regexp_extract_all(value: object, *, pattern: str, group: int = 1) -> Expression:
    """Return all matches of a literal regular expression capture group."""
    argument = _string_argument(value, "regexp_extract_all(...)")
    _string_literal(pattern, "regexp_extract_all(...)", "pattern")
    if isinstance(group, bool) or not isinstance(group, int) or group < 0:
        raise TypeError("regexp_extract_all(...) group must be a non-negative integer")
    return Expression(
        kind="call",
        type=ArrayType(StringType(), contains_null=False),
        nullable=argument.nullable,
        data={"function": "regexp_extract_all", "pattern": pattern, "group": group},
        args=(argument,),
    )


def regexp_instr(value: object, *, pattern: str, group: int = 0) -> Expression:
    """Return the one-based start position of a literal regular-expression match."""
    argument = _string_argument(value, "regexp_instr(...)")
    _string_literal(pattern, "regexp_instr(...)", "pattern")
    if isinstance(group, bool) or not isinstance(group, int) or group < 0:
        raise TypeError("regexp_instr(...) group must be a non-negative integer")
    return Expression(
        kind="call", type=IntegerType(), nullable=argument.nullable,
        data={"function": "regexp_instr", "pattern": pattern, "group": group}, args=(argument,)
    )


def regexp_substr(value: object, *, pattern: str) -> Expression:
    """Return the first substring matching a literal regular expression."""
    argument = _string_argument(value, "regexp_substr(...)")
    _string_literal(pattern, "regexp_substr(...)", "pattern")
    return Expression(
        kind="call", type=StringType(), nullable=True,
        data={"function": "regexp_substr", "pattern": pattern}, args=(argument,)
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


def bit_length(value: object) -> Expression:
    """Return the number of bits in a String or Binary expression."""
    argument = _string_or_binary_argument(value, "bit_length(...)")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=argument.nullable,
        data={"function": "bit_length"},
        args=(argument,),
    )


def ascii(value: object) -> Expression:
    """Return the numeric value of the first character in a string."""
    argument = _string_argument(value, "ascii(...)")
    return Expression(kind="call", type=IntegerType(), nullable=argument.nullable, data={"function": "ascii"}, args=(argument,))


def char(value: object) -> Expression:
    """Return the character represented by an integral expression."""
    argument = _integral_argument(value, "char(...)")
    return Expression(
        kind="call", type=StringType(), nullable=argument.nullable, data={"function": "char"}, args=(argument,)
    )


def char_length(value: object) -> Expression:
    """Return the character length of a string expression."""
    argument = _string_argument(value, "char_length(...)")
    return Expression(
        kind="call", type=IntegerType(), nullable=argument.nullable, data={"function": "char_length"}, args=(argument,)
    )


def character_length(value: object) -> Expression:
    """Return character length using PySpark's ``character_length`` spelling."""
    argument = _string_argument(value, "character_length(...)")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=argument.nullable,
        data={"function": "character_length"},
        args=(argument,),
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


def find_in_set(value: object, values: object) -> Expression:
    """Return the one-based position of a string in a comma-delimited string."""
    arguments = (_string_argument(value, "find_in_set(...)"), _string_argument(values, "find_in_set(...)"))
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=any(argument.nullable for argument in arguments),
        data={"function": "find_in_set"},
        args=arguments,
    )


def equal_null(left: object, right: object) -> Expression:
    """Compare two values with Spark's null-safe equality semantics."""
    return literal(left).null_safe_eq(right)


def assert_true(condition: object, *, message: str | None = None) -> Expression:
    """Fail Spark evaluation unless ``condition`` is true.

    The returned Boolean guard is true after the native PySpark assertion has
    succeeded, which lets it be used directly in ``where(...)``.
    """
    expression = literal(condition)
    if not isinstance(expression.type, BooleanType):
        raise TypeError("assert_true(condition) requires a Boolean Structure expression")
    if message is not None and not isinstance(message, str):
        raise TypeError("assert_true(message=...) must be a string literal or None")
    return Expression(
        kind="assertion",
        type=BooleanType(),
        nullable=False,
        data={"function": "assert_true", "message": message},
        args=(expression,),
    )


def raise_error(message: str) -> Expression:
    """Return a Boolean expression that raises the supplied Spark error when evaluated."""
    if not isinstance(message, str):
        raise TypeError("raise_error(message) requires a string literal")
    return Expression(
        kind="assertion",
        type=BooleanType(),
        nullable=False,
        data={"function": "raise_error", "message": message},
    )


def like(value: object, pattern: object) -> Expression:
    """Match a String expression against a SQL LIKE pattern."""
    return _string_match_call("like", value, pattern)


def ilike(value: object, pattern: object) -> Expression:
    """Match a String expression against a case-insensitive SQL LIKE pattern."""
    return _string_match_call("ilike", value, pattern)


def regexp(value: object, pattern: object) -> Expression:
    """Match a String expression against a regular expression."""
    return _string_match_call("regexp", value, pattern)


def regexp_like(value: object, pattern: object) -> Expression:
    """Match a String expression against a regular expression."""
    return _string_match_call("regexp_like", value, pattern)


def rlike(value: object, pattern: object) -> Expression:
    """Match a String expression against a regular expression."""
    return _string_match_call("rlike", value, pattern)


def startswith(value: object, prefix: object) -> Expression:
    """Return whether a String or Binary expression starts with ``prefix``."""
    return _string_or_binary_match_call("startswith", value, prefix)


def endswith(value: object, suffix: object) -> Expression:
    """Return whether a String or Binary expression ends with ``suffix``."""
    return _string_or_binary_match_call("endswith", value, suffix)


def format_number(value: object, *, decimals: int) -> Expression:
    """Format a numeric expression with a literal number of decimal places."""
    argument = _numeric_argument(value, "format_number(...)")
    if isinstance(decimals, bool) or not isinstance(decimals, int) or decimals < 0:
        raise TypeError("format_number(...) decimals must be a non-negative integer literal")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "format_number", "decimals": decimals},
        args=(argument,),
    )


def position(substring: object, value: object, *, start: int = 1) -> Expression:
    """Return the one-based position of a substring after a literal start."""
    substring_argument = _string_argument(substring, "position(...)")
    value_argument = _string_argument(value, "position(...)")
    if isinstance(start, bool) or not isinstance(start, int) or start < 1:
        raise TypeError("position(...) start must be a positive integer literal")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=substring_argument.nullable or value_argument.nullable,
        data={"function": "position", "start": start},
        args=(substring_argument, value_argument),
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


def split_part(value: object, delimiter: object, part_num: object) -> Expression:
    """Return a one-based or negative-indexed delimiter-separated part."""
    source = _string_argument(value, "split_part(...)")
    separator = _string_argument(delimiter, "split_part(...)")
    part = _integral_argument(part_num, "split_part(...)")
    if part.kind == "literal" and part.data is not None and part.data.get("value") == 0:
        raise ValueError("split_part(...) part_num cannot be zero")
    return Expression(
        kind="call",
        type=StringType(),
        nullable=source.nullable or separator.nullable or part.nullable,
        data={"function": "split_part"},
        args=(source, separator, part),
    )


def initcap(value: object) -> Expression:
    """Title-case words in a string expression, like Spark ``initcap``."""
    return _string_call("initcap", value)


def reverse(value: object) -> Expression:
    """Reverse a string expression, like Spark ``reverse`` for strings."""
    return _string_call("reverse", value)


def soundex(value: object) -> Expression:
    """Return the SoundEx code for a string expression."""
    return _string_call("soundex", value)


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


def crc32(value: object) -> Expression:
    """Return the CRC-32 checksum of a string or binary expression."""
    argument = literal(value)
    if not isinstance(argument.type, (StringType, BinaryType)):
        raise TypeError("crc32(...) requires a String or Binary expression")
    return Expression(
        kind="call",
        type=LongType(),
        nullable=argument.nullable,
        data={"function": "crc32"},
        args=(argument,),
    )


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


def make_date(year: object, month: object, day: object) -> Expression:
    """Build a Date from typed year, month, and day expressions.

    Args:
        year: Integer or Long expression for the year.
        month: Integer or Long expression for the month.
        day: Integer or Long expression for the day of the month.

    Returns:
        A nullable Date expression, following PySpark ``make_date``. Invalid
        calendar components return null or raise during execution according to
        Spark's ``spark.sql.ansi.enabled`` setting.

    Example:
        order_date = make_date(order.year, order.month, order.day)
    """
    arguments = tuple(_integral_argument(value, "make_date(...)") for value in (year, month, day))
    return Expression(
        kind="call",
        type=DateType(),
        nullable=True,
        data={"function": "make_date"},
        args=arguments,
    )


def make_timestamp_ltz(
    year: object, month: object, day: object, hour: object, minute: object, second: object,
    *, timezone: object | None = None,
) -> Expression:
    """Build a nullable instant timestamp from typed calendar components.

    Args:
        year: Integral year expression.
        month: Integral month expression.
        day: Integral day expression.
        hour: Integral hour expression.
        minute: Integral minute expression.
        second: Numeric seconds expression; Decimal preserves exact fractions.
        timezone: Optional String expression; omitted uses Spark's session zone.

    Returns:
        Nullable LTZ Timestamp. Invalid components follow Spark's ANSI policy.
    """
    return _make_timestamp("make_timestamp_ltz", (year, month, day, hour, minute, second), timezone)


def make_timestamp_ntz(
    year: object, month: object, day: object, hour: object, minute: object, second: object,
) -> Expression:
    """Build a nullable wall-clock timestamp from typed calendar components."""
    return _make_timestamp("make_timestamp_ntz", (year, month, day, hour, minute, second), None)


def make_timestamp(
    year: object, month: object, day: object, hour: object, minute: object, second: object,
    *, timezone: object | None = None,
) -> Expression:
    """Build a timestamp using the resolved Spark timestamp type."""
    result = _make_timestamp("make_timestamp", (year, month, day, hour, minute, second), timezone)
    if isinstance(result.type, TimestampNTZType) and timezone is not None:
        return Expression(kind=result.kind, type=result.type, nullable=result.nullable,
                          data={**(result.data or {}), "warnings": ("PYSPARK-W2706",)}, args=result.args)
    return result


def _make_timestamp(function: str, values: tuple[object, ...], timezone: object | None) -> Expression:
    components = tuple(_integral_argument(value, f"{function}(...)") for value in values[:5])
    seconds = _numeric_argument(values[5], f"{function}(...) seconds")
    zone = () if timezone is None else (_string_argument(timezone, f"{function}(...) timezone"),)
    return Expression(
        kind="call",
        type=(
            TimestampNTZType()
            if function == "make_timestamp_ntz" or (function == "make_timestamp" and _generic_timestamp_is_ntz())
            else TimestampType()
        ),
        nullable=True,
        data={"function": function},
        args=(*components, seconds, *zone),
    )


def make_ym_interval(years: object = 0, months: object = 0) -> Expression:
    """Build Spark's year-to-month interval from integral components."""
    return _interval_call("make_ym_interval", IntervalType("year_month", "year_to_month"), (years, months))


def make_dt_interval(days: object = 0, hours: object = 0, mins: object = 0, secs: object = 0) -> Expression:
    """Build Spark's day-to-second interval from integral and numeric components."""
    return _interval_call("make_dt_interval", IntervalType("day_time", "day_to_second"), (days, hours, mins, secs))


def make_interval(
    years: object = 0, months: object = 0, weeks: object = 0, days: object = 0,
    hours: object = 0, mins: object = 0, secs: object = 0,
) -> Expression:
    """Build a mixed calendar interval using Spark's seven components."""
    return _interval_call("make_interval", IntervalType("calendar", "calendar"),
                          (years, months, weeks, days, hours, mins, secs))


def interval(*, type: str | None = None, unit: str | None = None, **components: object) -> Expression:
    """Build one exact interval qualifier from the corresponding named components.

    For example, ``interval(type=Interval.YEAR_TO_MONTH, years=y, months=m)``
    requires both components; ``interval(unit=Interval.DAY, days=d)`` requires one.
    """
    from structure.plugin.pyspark.dsl.types import interval as interval_type

    result_type = interval_type(type=type, unit=unit)
    fields = {
        "year": "years", "month": "months", "day": "days", "hour": "hours",
        "minute": "mins", "second": "secs",
    }
    if result_type.kind == "calendar":
        selected: tuple[str, ...] = ("years", "months", "weeks", "days", "hours", "mins", "secs")
    else:
        ordered = ("year", "month") if result_type.kind == "year_month" else ("day", "hour", "minute", "second")
        assert result_type.start_field is not None and result_type.end_field is not None
        selected = tuple(fields[name] for name in ordered[result_type.start_field:result_type.end_field + 1])
    if set(components) != set(selected):
        raise TypeError(f"interval(...) {result_type.qualifier} requires exactly: {', '.join(selected)}")
    all_fields = ("years", "months") if result_type.kind == "year_month" else (
        ("days", "hours", "mins", "secs") if result_type.kind == "day_time" else
        ("years", "months", "weeks", "days", "hours", "mins", "secs")
    )
    values = tuple(components.get(name, 0) for name in all_fields)
    return _interval_call("interval", result_type, values)


def _interval_call(function: str, result_type: IntervalType, values: tuple[object, ...]) -> Expression:
    arguments = tuple(
        _numeric_argument(value, f"{function}(...) seconds")
        if index == len(values) - 1 and result_type.kind != "year_month"
        else _integral_argument(value, f"{function}(...)")
        for index, value in enumerate(values)
    )
    return Expression(kind="call", type=result_type, nullable=any(arg.nullable for arg in arguments),
                      data={"function": function}, args=arguments)


def date_sub(value: object, *, days: object) -> Expression:
    """Subtract whole days from a Date or Timestamp expression.

    Args:
        value: Date or Timestamp expression.
        days: Integer literal or integral Structure expression.

    Returns:
        A Date expression, following PySpark ``date_sub``.
    """
    argument = _date_or_timestamp_argument(value, "date_sub(...)")
    if isinstance(days, bool):
        raise TypeError("date_sub(...) days must be an integer or integral Structure expression")
    if isinstance(days, int):
        return Expression(
            kind="call",
            type=DateType(),
            nullable=argument.nullable,
            data={"function": "date_sub", "days": days},
            args=(argument,),
        )
    day_count = literal(days)
    if not isinstance(day_count.type, (IntegerType, LongType)):
        raise TypeError("date_sub(...) days must be an integer or integral Structure expression")
    return Expression(
        kind="call",
        type=DateType(),
        nullable=argument.nullable or day_count.nullable,
        data={"function": "date_sub"},
        args=(argument, day_count),
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


def date_diff(end: object, start: object) -> Expression:
    """Return the number of days from ``start`` to ``end`` using Spark's alias."""
    result = datediff(end, start)
    return Expression(kind="call", type=result.type, nullable=result.nullable, data={"function": "date_diff"}, args=result.args)


def dateadd(value: object, *, days: object) -> Expression:
    """Add whole days to a Date or Timestamp using Spark's alias."""
    result = date_add(value, days=days)
    return Expression(kind="call", type=result.type, nullable=result.nullable, data={**(result.data or {}), "function": "dateadd"}, args=result.args)


def day(value: object) -> Expression:
    """Extract the day of month from a Date or Timestamp."""
    return _calendar_part("day", value, _date_or_timestamp_argument)


def months_between(left: object, right: object, *, round_off: bool = True) -> Expression:
    """Return the fractional month difference between two Date or Timestamp expressions."""
    left_argument = _date_or_timestamp_argument(left, "months_between(...)")
    right_argument = _date_or_timestamp_argument(right, "months_between(...)")
    if not isinstance(round_off, bool):
        raise TypeError("months_between(...) round_off must be a Boolean literal")
    return Expression(
        kind="call",
        type=DoubleType(),
        nullable=left_argument.nullable or right_argument.nullable,
        data={"function": "months_between", "round_off": round_off},
        args=(left_argument, right_argument),
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


def dayofweek(value: object) -> Expression:
    """Extract the day of week from a Date or Timestamp expression.

    Spark numbers Sunday as 1 and Saturday as 7.
    """
    return _calendar_part("dayofweek", value, _date_or_timestamp_argument)


def weekday(value: object) -> Expression:
    """Extract the zero-based Monday-first weekday from a Date or Timestamp expression."""
    return _calendar_part("weekday", value, _date_or_timestamp_argument)


def dayofyear(value: object) -> Expression:
    """Extract the day of year from a Date or Timestamp expression."""
    return _calendar_part("dayofyear", value, _date_or_timestamp_argument)


def quarter(value: object) -> Expression:
    """Extract the calendar quarter from a Date or Timestamp expression."""
    return _calendar_part("quarter", value, _date_or_timestamp_argument)


def weekofyear(value: object) -> Expression:
    """Extract the ISO week number from a Date or Timestamp expression."""
    return _calendar_part("weekofyear", value, _date_or_timestamp_argument)


def last_day(value: object) -> Expression:
    """Return the last day of the month containing a Date or Timestamp."""
    argument = _date_or_timestamp_argument(value, "last_day(...)")
    return Expression(
        kind="call", type=DateType(), nullable=argument.nullable, data={"function": "last_day"}, args=(argument,)
    )


def date_format(value: object, *, format: str) -> Expression:
    """Format a Date or Timestamp expression with a non-empty Spark pattern."""
    argument = _date_or_timestamp_argument(value, "date_format(...)")
    format_literal = _temporal_format(format, "date_format(...)")
    assert format_literal is not None
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "date_format", "format": format_literal},
        args=(argument,),
    )


def mask(
    value: object,
    *,
    upper_char: str | None = None,
    lower_char: str | None = None,
    digit_char: str | None = None,
    other_char: str | None = None,
) -> Expression:
    """Mask upper/lower-case letters and digits in a String expression."""
    argument = _string_argument(value, "mask(...)")
    chars = tuple(
        _mask_character(character, parameter, "mask(...)")
        for parameter, character in (
            ("upper_char", upper_char),
            ("lower_char", lower_char),
            ("digit_char", digit_char),
            ("other_char", other_char),
        )
    )
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "mask", "chars": chars},
        args=(argument,),
    )


def overlay(value: object, replace: object, *, pos: object, length: object = -1) -> Expression:
    """Replace part of a String or Binary expression at a typed position."""
    argument = _string_or_binary_argument(value, "overlay(...)")
    replacement = _string_or_binary_argument(replace, "overlay(...)")
    if not isinstance(argument.type, type(replacement.type)):
        raise TypeError("overlay(...) value and replace must use the same String or Binary type")
    position = _integral_argument(pos, "overlay(...)")
    replace_length = _integral_argument(length, "overlay(...)")
    return Expression(
        kind="call",
        type=argument.type,
        nullable=argument.nullable or replacement.nullable or position.nullable or replace_length.nullable,
        data={"function": "overlay"},
        args=(argument, replacement, position, replace_length),
    )


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


def to_timestamp(value: object, *, format: object | None = None) -> Expression:
    """Convert a String, Date, or Timestamp expression to Timestamp."""
    argument = _temporal_conversion_argument(value, "to_timestamp(...)")
    data: dict[str, object] = {"function": "to_timestamp"}
    arguments: tuple[Expression, ...] = (argument,)
    if isinstance(format, str):
        pattern = _temporal_format(format, "to_timestamp(...)")
        data["format"] = pattern
    elif format is not None:
        arguments = (argument, _string_argument(format, "to_timestamp(...) format"))
    if format is not None and not isinstance(argument.type, StringType):
        data["warnings"] = ("PYSPARK-W2705",)
    return Expression(
        kind="call",
        type=TimestampNTZType() if _generic_timestamp_is_ntz() else TimestampType(),
        nullable=True if isinstance(argument.type, StringType) else argument.nullable,
        data=data,
        args=arguments,
    )


def try_to_timestamp(value: object, *, format: object | None = None) -> Expression:
    """Convert a String, Date, or LTZ Timestamp, returning null for invalid text."""
    source = _temporal_conversion_argument(value, "try_to_timestamp(...)")
    arguments = (source,) if format is None else (source, _string_argument(format, "try_to_timestamp(...) format"))
    data: dict[str, object] = {"function": "try_to_timestamp"}
    if format is not None and not isinstance(source.type, StringType):
        data["warnings"] = ("PYSPARK-W2705",)
    return Expression(kind="call", type=TimestampNTZType() if _generic_timestamp_is_ntz() else TimestampType(),
                      nullable=True, data=data, args=arguments)


def _generic_timestamp_is_ntz() -> bool:
    return current_compilation_settings().get("spark.sql.timestampType", "TIMESTAMP_LTZ") == "TIMESTAMP_NTZ"


def to_timestamp_ntz(value: object, *, format: object | None = None) -> Expression:
    """Parse a String expression as a timestamp without time zone.

    Args:
        value: String expression containing the timestamp text.
        format: Optional String expression containing Spark's datetime pattern.

    Returns:
        Nullable TimestampNTZ. Invalid input follows Spark's ANSI setting;
        use ``try_to_timestamp`` when parse failures must become null.

    Example:
        local_time = to_timestamp_ntz(row.raw_time, format=row.pattern)
    """
    source = _string_argument(value, "to_timestamp_ntz(...)")
    arguments = (source,) if format is None else (
        source,
        _string_argument(format, "to_timestamp_ntz(...) format"),
    )
    return Expression(
        kind="call",
        type=TimestampNTZType(),
        nullable=True,
        data={"function": "to_timestamp_ntz"},
        args=arguments,
    )


def to_timestamp_ltz(value: object, *, format: object | None = None) -> Expression:
    """Parse String input into a nullable instant timestamp with a typed pattern."""
    source = _string_argument(value, "to_timestamp_ltz(...)")
    arguments = (source,) if format is None else (source, _string_argument(format, "to_timestamp_ltz(...) format"))
    return Expression(kind="call", type=TimestampType(), nullable=True, data={"function": "to_timestamp_ltz"}, args=arguments)


def unix_date(value: object) -> Expression:
    """Return the number of days since 1970-01-01 for a Date expression."""
    argument = _date_argument(value, "unix_date(...)")
    return Expression(
        kind="call", type=IntegerType(), nullable=argument.nullable, data={"function": "unix_date"}, args=(argument,)
    )


def date_from_unix_date(days: object) -> Expression:
    """Convert whole days since 1970-01-01 to a Date expression."""
    argument = _integral_argument(days, "date_from_unix_date(...)")
    return Expression(
        kind="call",
        type=DateType(),
        nullable=argument.nullable,
        data={"function": "date_from_unix_date"},
        args=(argument,),
    )


def from_unixtime(seconds: object, *, format: str = "yyyy-MM-dd HH:mm:ss") -> Expression:
    """Format Unix epoch seconds in the Spark session time zone."""
    argument = _numeric_argument(seconds, "from_unixtime(...)")
    format_literal = _temporal_format(format, "from_unixtime(...)")
    assert format_literal is not None
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable,
        data={"function": "from_unixtime", "format": format_literal},
        args=(argument,),
    )


def unix_timestamp(
    value: object | None = None,
    *,
    format: object | None = None,
) -> Expression:
    """Parse a temporal expression into Unix epoch seconds.

    Omitting ``value`` uses Spark's query-time current timestamp. Explicit
    String, Date, and Timestamp inputs are parsed with the requested format.
    """
    if value is None:
        format_literal = _temporal_format("yyyy-MM-dd HH:mm:ss" if format is None else format, "unix_timestamp(...)")
        assert format_literal is not None
        return Expression(
            kind="call",
            type=LongType(),
            nullable=False,
            data={
                "function": "unix_timestamp",
                "format": format_literal,
                "nondeterministic": True,
                "query_stable": True,
            },
        )
    argument = _temporal_conversion_argument(value, "unix_timestamp(...)")
    data: dict[str, object]
    arguments: tuple[Expression, ...]
    if format is None or isinstance(format, str):
        format_literal = _temporal_format("yyyy-MM-dd HH:mm:ss" if format is None else format, "unix_timestamp(...)")
        assert format_literal is not None
        data = {"function": "unix_timestamp", "format": format_literal}
        arguments = (argument,)
    else:
        pattern = _string_argument(format, "unix_timestamp(...) format")
        data = {"function": "unix_timestamp"}
        arguments = (argument, pattern)
    if format is not None and not isinstance(argument.type, StringType):
        data["warnings"] = ("PYSPARK-W2705",)
    return Expression(
        kind="call",
        type=LongType(),
        nullable=True if isinstance(argument.type, StringType) else argument.nullable,
        data=data,
        args=arguments,
    )


def to_unix_timestamp(value: object, *, format: object | None = None) -> Expression:
    """Parse a required String, Date, or Timestamp value into epoch seconds."""
    source = _temporal_conversion_argument(value, "to_unix_timestamp(...)")
    arguments = (source,) if format is None else (source, _string_argument(format, "to_unix_timestamp(...) format"))
    data: dict[str, object] = {"function": "to_unix_timestamp"}
    if format is not None and not isinstance(source.type, StringType):
        data["warnings"] = ("PYSPARK-W2705",)
    return Expression(
        kind="call", type=LongType(), nullable=True if isinstance(source.type, StringType) else source.nullable,
        data=data, args=arguments,
    )


def timestamp_seconds(value: object) -> Expression:
    """Turn numeric Unix seconds, including fractions, into an LTZ Timestamp."""
    argument = _numeric_argument(value, "timestamp_seconds(...)")
    return Expression(
        kind="call", type=TimestampType(),
        nullable=argument.nullable or isinstance(argument.type, (FloatType, DoubleType)),
        data={"function": "timestamp_seconds"}, args=(argument,),
    )


def timestamp_millis(value: object) -> Expression:
    """Turn integral Unix milliseconds into an LTZ Timestamp."""
    return _epoch_to_timestamp("timestamp_millis", value)


def timestamp_micros(value: object) -> Expression:
    """Turn integral Unix microseconds into an LTZ Timestamp."""
    return _epoch_to_timestamp("timestamp_micros", value)


def _epoch_to_timestamp(function: str, value: object) -> Expression:
    argument = _integral_argument(value, f"{function}(...)")
    return Expression(kind="call", type=TimestampType(), nullable=argument.nullable, data={"function": function}, args=(argument,))


def unix_seconds(value: object) -> Expression:
    """Return integral epoch seconds for an LTZ Timestamp."""
    return _timestamp_to_epoch("unix_seconds", value)


def unix_millis(value: object) -> Expression:
    """Return integral epoch milliseconds for an LTZ Timestamp."""
    return _timestamp_to_epoch("unix_millis", value)


def unix_micros(value: object) -> Expression:
    """Return integral epoch microseconds for an LTZ Timestamp."""
    return _timestamp_to_epoch("unix_micros", value)


def _timestamp_to_epoch(function: str, value: object) -> Expression:
    argument = _timestamp_argument(value, f"{function}(...)")
    return Expression(kind="call", type=LongType(), nullable=argument.nullable, data={"function": function}, args=(argument,))


def to_utc_timestamp(value: object, *, timezone: str) -> Expression:
    """Convert a timestamp expression from a timezone to UTC."""
    return _timezone_conversion("to_utc_timestamp", value, timezone)


def from_utc_timestamp(value: object, *, timezone: str) -> Expression:
    """Convert a UTC timestamp expression to a timezone."""
    return _timezone_conversion("from_utc_timestamp", value, timezone)


def convert_timezone(source_tz: object | None, target_tz: object, source_ts: object) -> Expression:
    """Convert a wall-clock timestamp between time zones, preserving TimestampNTZ semantics."""
    timestamp = _timestamp_ntz_argument(source_ts, "convert_timezone(...)")
    target = _string_argument(target_tz, "convert_timezone(...)")
    source = None if source_tz is None else _string_argument(source_tz, "convert_timezone(...)")
    arguments = (target, timestamp) if source is None else (source, target, timestamp)
    return Expression(
        kind="call",
        type=TimestampNTZType(),
        nullable=any(argument.nullable for argument in arguments),
        data={"function": "convert_timezone", "source_tz_default": source is None},
        args=arguments,
    )


def date_part(field: object, value: object) -> Expression:
    """Extract a named date or timestamp part with a compiler-visible field."""
    return _date_part("date_part", field, value)


def datepart(field: object, value: object) -> Expression:
    """Alias for :func:`date_part`, matching PySpark's SQL spelling."""
    return _date_part("datepart", field, value)


def extract(field: object, value: object) -> Expression:
    """Extract a compiler-visible Spark date, timestamp, or interval part."""
    return _date_part("extract", field, value)


def abs(value: object) -> Expression:
    """Return the absolute value of a numeric expression."""
    argument = _numeric_argument(value, "abs(...)")
    return Expression(
        kind="call", type=argument.type, nullable=argument.nullable, data={"function": "abs"}, args=(argument,)
    )


def bit_count(value: object) -> Expression:
    """Return the number of set bits in an integral expression."""
    argument = _integral_argument(value, "bit_count(...)")
    return Expression(
        kind="call", type=LongType(), nullable=argument.nullable, data={"function": "bit_count"}, args=(argument,)
    )


def bitwise_not(value: object) -> Expression:
    """Return the integral bitwise complement, preserving PySpark's function spelling."""
    argument = _integral_argument(value, "bitwise_not(...)")
    return Expression(kind="bitwise_not", type=argument.type, nullable=argument.nullable, args=(argument,))


def bit_get(value: object, position: object) -> Expression:
    """Return the bit at an integral position in an integral expression."""
    return _bit_position_call("bit_get", value, position)


def getbit(value: object, position: object) -> Expression:
    """Alias for :func:`bit_get`, matching Spark's SQL spelling."""
    return _bit_position_call("getbit", value, position)


def shiftleft(value: object, *, bits: int) -> Expression:
    """Shift an integral expression left by an integer literal count."""
    return _bit_shift_call("shiftleft", value, bits)


def shiftright(value: object, *, bits: int) -> Expression:
    """Shift an integral expression right by an integer literal count."""
    return _bit_shift_call("shiftright", value, bits)


def shiftrightunsigned(value: object, *, bits: int) -> Expression:
    """Shift an integral expression right without sign extension."""
    return _bit_shift_call("shiftrightunsigned", value, bits)


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
    return _random_call("rand", seed=seed, reproducible=reproducible)


def randn(*, seed: int | None = None, reproducible: bool = True) -> Expression:
    """Generate a non-null standard-normal random Double expression."""
    return _random_call("randn", seed=seed, reproducible=reproducible)


def _random_call(function: str, *, seed: int | None, reproducible: bool) -> Expression:
    if not isinstance(reproducible, bool):
        raise TypeError(f"{function}(...) reproducible must be a Boolean")
    if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
        raise TypeError(f"{function}(...) seed must be an integer literal or None")
    if reproducible and seed is None:
        raise TypeError(f"{function}(...) seed is required unless reproducible=False")
    return Expression(
        kind="call",
        type=DoubleType(),
        nullable=False,
        data={
            "function": function,
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


def ceiling(value: object) -> Expression:
    """Return the ceiling of a numeric expression, preserving PySpark's name."""
    argument = _numeric_argument(value, "ceiling(...)")
    return Expression(
        kind="call",
        type=_ceiling_type(argument.type),
        nullable=argument.nullable,
        data={"function": "ceiling"},
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


def power(value: object, exponent: object) -> Expression:
    """Raise a numeric expression to a power, preserving PySpark's name."""
    base = _numeric_argument(value, "power(...)")
    exponent_argument = _numeric_argument(exponent, "power(...)")
    return Expression(
        kind="call",
        type=DoubleType(),
        nullable=base.nullable or exponent_argument.nullable,
        data={"function": "power"},
        args=(base, exponent_argument),
    )


def negate(value: object) -> Expression:
    """Negate a numeric expression using PySpark's ``negate`` function name."""
    return _numeric_unary_call("negate", value)


def negative(value: object) -> Expression:
    """Negate a numeric expression using PySpark's ``negative`` function name."""
    return _numeric_unary_call("negative", value)


def positive(value: object) -> Expression:
    """Return a numeric expression using PySpark's ``positive`` function name."""
    return _numeric_unary_call("positive", value)


def pi() -> Expression:
    """Return pi as a non-null Double expression."""
    return _constant_double_call("pi")


def unhex(value: object) -> Expression:
    """Decode a hexadecimal String expression into nullable Binary."""
    argument = _string_argument(value, "unhex(...)")
    return Expression(kind="call", type=BinaryType(), nullable=True, data={"function": "unhex"}, args=(argument,))


def width_bucket(value: object, minimum: object, maximum: object, *, num_buckets: int) -> Expression:
    """Return a nullable Long histogram bucket for compatible numeric values."""
    arguments = tuple(_numeric_argument(item, "width_bucket(...)") for item in (value, minimum, maximum))
    if any(argument.type is None for argument in arguments):
        raise AssertionError("numeric argument validation must reject untyped expressions")
    _positive_integer_literal(num_buckets, "width_bucket(...) num_buckets")
    _common_numeric_type("width_bucket(...)", tuple(argument.type for argument in arguments if argument.type is not None))
    return Expression(
        kind="call",
        type=LongType(),
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


_PARTITIONS_UNSET = object()


@overload
def coalesce(value: object, fallback: object, /, *values: object) -> Expression: ...


@overload
def coalesce(*, partitions: int) -> RowScope: ...


def coalesce(
    *values: object,
    partitions: object = _PARTITIONS_UNSET,
) -> Expression | RowScope:
    """Return the first non-null value using Structure common-type rules.

    Args:
        *values: At least two compatible scalar expressions or literals.
        partitions: Keyword-only positive partition count for relation coalescing.

    Returns:
        A typed scalar expression, or the current row scope after relation coalescing.

    Example:
        display_name = coalesce(customer.nickname, customer.full_name, "unknown")
        fewer_partitions = coalesce(partitions=4)
    """
    if partitions is not _PARTITIONS_UNSET:
        if values:
            raise TypeError("coalesce(partitions=...) cannot be combined with scalar values")
        from structure.plugin.pyspark.dsl.relation_sets import _coalesce_partitions

        return _coalesce_partitions(partitions)
    if not values:
        raise TypeError("coalesce(...) requires at least two scalar values or the keyword partitions=...")
    if len(values) < 2:
        raise TypeError("scalar coalesce(...) requires at least two values; use the value directly")
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


def _string_match_call(function: str, value: object, pattern: object) -> Expression:
    arguments = tuple(
        _string_argument(argument, f"{function}(...)")
        for argument in (value, pattern)
    )
    return Expression(
        kind="call",
        type=BooleanType(),
        nullable=any(argument.nullable for argument in arguments),
        data={"function": function},
        args=arguments,
    )


def _string_or_binary_match_call(function: str, value: object, pattern: object) -> Expression:
    arguments = tuple(_string_or_binary_argument(argument, f"{function}(...)") for argument in (value, pattern))
    return Expression(
        kind="call",
        type=BooleanType(),
        nullable=any(argument.nullable for argument in arguments),
        data={"function": function},
        args=arguments,
    )


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
        raise TypeError(f"{call} to= must be a Schema class")
    return value


def _parser_schema_argument(value: object, call: str):
    schema = _schema_argument(value, call)
    _parser_nullable_schema(schema, call, path=schema.__name__)
    return schema


def _parser_nullable_schema(schema: Any, call: str, *, path: str) -> None:
    for field in schema._structure_fields.values():
        field_path = f"{path}.{field.name}"
        if not field.nullable:
            raise TypeError(f"{call} to= schema field {field_path} must be nullable for permissive parsing")
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


def _binary_format(value: object, call: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise TypeError(f"{call} format must be one of: hex, utf-8, utf8, base64")
    normalized = value.lower()
    if normalized not in {"hex", "utf-8", "utf8", "base64"}:
        raise TypeError(f"{call} format must be one of: hex, utf-8, utf8, base64")
    return normalized


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


def _substring_call(
    function: str, value: object, *, start: object, length: object, method: bool = False
) -> Expression:
    argument = _string_argument(value, f"{function}(...)")
    start_argument = _integral_argument(start, f"{function}(...)")
    length_argument = _integral_argument(length, f"{function}(...)")
    if start_argument.kind == "literal" and start_argument.data is not None:
        _positive_integer_literal(start_argument.data["value"], f"{function}(...) start")
    if length_argument.kind == "literal" and length_argument.data is not None:
        length_value = length_argument.data["value"]
        if isinstance(length_value, int) and length_value < 0:
            raise TypeError(f"{function}(...) length must be a non-negative integer")
    data: dict[str, object] = {"function": function}
    if method:
        data["method"] = True
    if start_argument.kind == "literal" and start_argument.data is not None:
        data["start"] = start_argument.data["value"]
    if length_argument.kind == "literal" and length_argument.data is not None:
        data["length"] = length_argument.data["value"]
    return Expression(
        kind="call",
        type=StringType(),
        nullable=argument.nullable or start_argument.nullable or length_argument.nullable,
        data=data,
        args=(argument, start_argument, length_argument),
    )


def _scalar_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if argument.type is None and argument.nullable:
        return argument
    if isinstance(
        argument.type,
        (
            BinaryType,
            BooleanType,
            DateType,
            TimestampType,
            TimestampNTZType,
            StringType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            DecimalType,
        ),
    ):
        return argument
    raise TypeError(f"{call} requires scalar Structure expressions")


def _format_string_call(function: str, format: object, values: tuple[object, ...]) -> Expression:
    if not isinstance(format, str):
        raise TypeError(f"{function}(...) format must be a string literal")
    arguments = tuple(_scalar_argument(value, f"{function}(...)") for value in values)
    return Expression(
        kind="call",
        type=StringType(),
        nullable=any(argument.nullable for argument in arguments),
        data={"function": function, "format": format},
        args=arguments,
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


def _mask_character(value: object, parameter: str, call: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) != 1:
        raise TypeError(f"{call} {parameter} must be a single-character string literal or None")
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
            TimestampNTZType,
        ),
    ):
        raise TypeError(f"{call} requires scalar Structure expressions")
    return argument


def _date_or_timestamp_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, (DateType, TimestampType, TimestampNTZType)):
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


def _timestamp_ntz_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, TimestampNTZType):
        raise TypeError(f"{call} requires a TimestampNTZ Structure expression")
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


def _timezone_conversion(function: str, value: object, timezone: str) -> Expression:
    argument = _temporal_conversion_argument(value, f"{function}(...)")
    if not isinstance(timezone, str) or not timezone:
        raise TypeError(f"{function}(...) timezone must be a non-empty string literal")
    return Expression(
        kind="call",
        type=TimestampType(),
        nullable=True if isinstance(argument.type, StringType) else argument.nullable,
        data={"function": function, "timezone": timezone},
        args=(argument,),
    )


def _date_part(function: str, field: object, value: object) -> Expression:
    if isinstance(field, Expression) and field.kind == "literal" and isinstance(field.type, StringType):
        field = (field.data or {}).get("value")
    if not isinstance(field, str) or not field.strip():
        raise TypeError(f"{function}(...) field must be a compiler-visible non-empty String literal")
    normalized = field.strip().lower()
    aliases = {
        "y": "year", "years": "year", "yr": "year", "yrs": "year",
        "qtr": "quarter", "mon": "month", "mons": "month", "months": "month",
        "w": "week", "weeks": "week", "d": "day", "days": "day",
        "dow": "dayofweek", "dow_iso": "dayofweek_iso", "dayofyear": "doy",
        "h": "hour", "hours": "hour", "hr": "hour", "hrs": "hour",
        "m": "minute", "min": "minute", "mins": "minute", "minutes": "minute",
        "s": "second", "sec": "second", "secs": "second", "seconds": "second",
    }
    normalized = aliases.get(normalized, normalized)
    temporal_fields = {"year", "yearofweek", "quarter", "month", "week", "day", "dayofweek", "dayofweek_iso", "doy", "hour", "minute", "second"}
    interval_fields = {"year", "month", "day", "hour", "minute", "second"}
    argument = literal(value)
    allowed = interval_fields if isinstance(argument.type, IntervalType) else temporal_fields
    if normalized not in allowed or not isinstance(argument.type, (DateType, TimestampType, TimestampNTZType, IntervalType)):
        raise TypeError(f"{function}(...) field is not supported for this source type: {field!r}")
    result_type = DecimalType(precision=8, scale=6) if normalized == "second" else IntegerType()
    return Expression(
        kind="call",
        type=result_type,
        nullable=argument.nullable,
        data={"function": function, "field": normalized},
        args=(argument,),
    )


def _numeric_argument(value: object, call: str) -> Expression:
    argument = literal(value)
    if argument.type is None or argument.type.name not in {"decimal", "double", "float", "integer", "long"}:
        raise TypeError(f"{call} requires a numeric Structure expression")
    return argument


def _numeric_unary_call(function: str, value: object) -> Expression:
    argument = _numeric_argument(value, f"{function}(...)")
    if argument.type is None:
        raise AssertionError("numeric argument validation must reject untyped expressions")
    return Expression(
        kind="call",
        type=argument.type,
        nullable=argument.nullable,
        data={"function": function},
        args=(argument,),
    )


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


def _bit_position_call(function: str, value: object, position: object) -> Expression:
    value_argument = _integral_argument(value, f"{function}(...)")
    position_argument = _integral_argument(position, f"{function}(...)")
    return Expression(
        kind="call",
        type=IntegerType(),
        nullable=value_argument.nullable or position_argument.nullable,
        data={"function": function},
        args=(value_argument, position_argument),
    )


def _bit_shift_call(function: str, value: object, bits: int) -> Expression:
    argument = _integral_argument(value, f"{function}(...)")
    if isinstance(bits, bool) or not isinstance(bits, int):
        raise TypeError(f"{function}(...) bits must be an integer literal")
    return Expression(
        kind="call",
        type=LongType(),
        nullable=argument.nullable,
        data={"function": function, "bits": bits},
        args=(argument,),
    )


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
