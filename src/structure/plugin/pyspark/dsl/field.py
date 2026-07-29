"""PySpark schema field factories.

The factories in this module let users declare Structure schemas with Spark
types while preserving ordinary Python type-checker ergonomics.  Each factory
returns a ``FieldDeclaration`` that carries a Structure type, nullability,
metadata, and a PySpark schema validator.
"""

from __future__ import annotations

import builtins
from datetime import date as python_date
from typing import TYPE_CHECKING, Any, Mapping, cast

from structure.dsl import FieldDeclaration, Schema
from structure.plugin.pyspark.dsl.types import (
    Array,
    Binary,
    Boolean,
    Date,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Map,
    String,
    Struct,
    StructureType,
    Timestamp,
)
from structure.plugin.pyspark.dsl.validation.ValidatePySparkSchemas import ValidatePySparkSchemas

_validate = ValidatePySparkSchemas().validate


def string(**options: object) -> Any:
    """Declare a Spark ``string`` field.

    Args:
        **options: Field options such as ``nullable``, ``alias``, ``metadata``,
            and ``description``.

    Returns:
        A Structure field declaration for use on a ``Schema`` class.

    Example:
        class Customer(Schema):
            id = string(nullable=False, alias="customer_id")
    """
    return _declare(String(), options)


def binary(**options: object) -> Any:
    """Declare a Spark ``binary`` field for byte payloads.

    Args:
        **options: Field options such as ``nullable``, ``alias``, ``metadata``,
            and ``description``.

    Returns:
        A Structure field declaration with Spark ``BinaryType`` metadata.

    Example:
        payload = binary(nullable=False)
    """
    return _declare(Binary(), options)


def integer(**options: object) -> Any:
    """Declare a Spark ``int`` field."""
    return _declare(Integer(), options)


def long(**options: object) -> Any:
    """Declare a Spark ``bigint`` field."""
    return _declare(Long(), options)


def double(**options: object) -> Any:
    """Declare a Spark ``double`` field."""
    return _declare(Double(), options)


def boolean(**options: object) -> Any:
    """Declare a Spark ``boolean`` field."""
    return _declare(Boolean(), options)


def timestamp(**options: object) -> Any:
    """Declare a Spark ``timestamp`` field."""
    return _declare(Timestamp(), options)


def decimal(precision: int, scale: int, **options: object) -> Any:
    """Declare a Spark ``decimal(precision, scale)`` field.

    Args:
        precision: Total number of decimal digits.
        scale: Number of digits after the decimal point.
        **options: Field options such as ``nullable`` and ``alias``.

    Returns:
        A Structure field declaration.

    Example:
        total = decimal(12, 2, nullable=False)
    """
    return _declare(Decimal(precision, scale), options)


if TYPE_CHECKING:
    class float(builtins.float):
        def __new__(cls, *args: object, **kwargs: object) -> Any: ...

    class date(python_date):
        def __new__(cls, *args: object, **kwargs: object) -> Any: ...
else:
    def float(**options: object) -> Any:
        """Declare a Spark ``float`` field."""
        return _declare(Float(), options)

    def date(**options: object) -> Any:
        """Declare a Spark ``date`` field."""
        return _declare(Date(), options)


def array(element: FieldDeclaration, *, contains_null: bool = True, **options: object) -> Any:
    """Declare a Spark array field from a nested field factory.

    Args:
        element: Nested field declaration, such as ``string()``.
        contains_null: Whether array elements may be null.
        **options: Field options for the array field itself.

    Returns:
        A Structure field declaration with Spark array type metadata.

    Example:
        tags = array(string(), contains_null=False)
    """
    return _declare(Array(_nested_type(element, "array"), contains_null=contains_null), options)


def map(key: FieldDeclaration, value: FieldDeclaration, *, value_contains_null: bool = True, **options: object) -> Any:
    """Declare a Spark map field from nested key and value field factories.

    Args:
        key: Nested field declaration for map keys.
        value: Nested field declaration for map values.
        value_contains_null: Whether map values may be null.
        **options: Field options for the map field itself.

    Returns:
        A Structure field declaration with Spark map type metadata.

    Example:
        attributes = map(string(), string(), value_contains_null=False)
    """
    return _declare(Map(_nested_type(key, "map key"), _nested_type(value, "map value"), value_contains_null=value_contains_null), options)


def struct(schema: type[Schema], **options: object) -> Any:
    """Declare a nested Spark struct field backed by a Structure schema.

    Args:
        schema: Nested ``Schema`` class.
        **options: Field options for the struct field itself.

    Returns:
        A Structure field declaration with Spark struct type metadata.

    Example:
        shipping_address = struct(Address)
    """
    return _declare(Struct(schema), options)


def _declare(type: StructureType, options: Mapping[str, object]) -> FieldDeclaration:
    unknown = set(options) - {"nullable", "alias", "metadata", "description"}
    if unknown:
        raise TypeError(f"field factory got unsupported option(s): {', '.join(sorted(unknown))}")
    nullable, alias = options.get("nullable", True), options.get("alias")
    metadata, description = options.get("metadata"), options.get("description")
    if not isinstance(nullable, bool):
        raise TypeError("field factory nullable must be a bool")
    if alias is not None and (not isinstance(alias, str) or not alias):
        raise ValueError("field alias must be a non-empty string when supplied")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise TypeError("field factory metadata must be a mapping or None")
    if description is not None and not isinstance(description, str):
        raise TypeError("field factory description must be a string or None")
    return FieldDeclaration(
        type,
        nullable=nullable,
        alias=cast(str | None, alias),
        metadata=metadata or {},
        description=description,
        validator=_validate,
        _options=frozenset(options),
    )


def _nested_type(declaration: FieldDeclaration, context: str) -> StructureType:
    if not isinstance(declaration, FieldDeclaration):
        raise TypeError(f"{context}(...) requires a nested field declaration such as string()")
    if declaration._options:
        if "nullable" in declaration._options:
            option = "contains_null=False" if context == "array" else "value_contains_null=False"
            raise TypeError(f"{context}(...) controls nested nullability with {option}")
        raise TypeError(f"{context}(...) nested declarations may only specify a type")
    return declaration.type


__all__ = [
    "array",
    "binary",
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
