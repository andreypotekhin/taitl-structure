from __future__ import annotations

import builtins
from datetime import date as python_date
from typing import TYPE_CHECKING, Any, Mapping, cast

from structure.dsl import FieldDeclaration, Schema
from structure.plugin.pyspark.dsl.types import (
    Array,
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
from structure.plugin.pyspark.dsl.ValidatePySparkSchemas import ValidatePySparkSchemas

_validate = ValidatePySparkSchemas().validate


def string(**options: object) -> Any: return _declare(String(), options)
def integer(**options: object) -> Any: return _declare(Integer(), options)
def long(**options: object) -> Any: return _declare(Long(), options)
def double(**options: object) -> Any: return _declare(Double(), options)
def boolean(**options: object) -> Any: return _declare(Boolean(), options)
def timestamp(**options: object) -> Any: return _declare(Timestamp(), options)
def decimal(precision: int, scale: int, **options: object) -> Any: return _declare(Decimal(precision, scale), options)


if TYPE_CHECKING:
    class float(builtins.float):
        def __new__(cls, *args: object, **kwargs: object) -> Any: ...

    class date(python_date):
        def __new__(cls, *args: object, **kwargs: object) -> Any: ...
else:
    def float(**options: object) -> Any: return _declare(Float(), options)
    def date(**options: object) -> Any: return _declare(Date(), options)


def array(element: FieldDeclaration, *, contains_null: bool = True, **options: object) -> Any:
    return _declare(Array(_nested_type(element, "array"), contains_null=contains_null), options)


def map(key: FieldDeclaration, value: FieldDeclaration, *, value_contains_null: bool = True, **options: object) -> Any:
    return _declare(Map(_nested_type(key, "map key"), _nested_type(value, "map value"), value_contains_null=value_contains_null), options)


def struct(schema: type[Schema], **options: object) -> Any: return _declare(Struct(schema), options)


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


__all__ = ["array", "boolean", "date", "decimal", "double", "float", "integer", "long", "map", "string", "struct", "timestamp"]
