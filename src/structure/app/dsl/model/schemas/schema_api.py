from __future__ import annotations

from typing import TYPE_CHECKING, Mapping, cast

from structure.app.dsl.model.schemas.FieldDeclaration import FieldDeclaration
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

if TYPE_CHECKING:
    from structure.app.dsl.model.schemas.Schema import Schema


def _require_alias(alias: str | None) -> None:
    if alias is not None and (not isinstance(alias, str) or not alias):
        raise ValueError("field alias must be a non-empty string when supplied")


def string(**options: object) -> FieldDeclaration:
    return _declare(String(), options)


def integer(**options: object) -> FieldDeclaration:
    return _declare(Integer(), options)


def long(**options: object) -> FieldDeclaration:
    return _declare(Long(), options)


def float(**options: object) -> FieldDeclaration:
    return _declare(Float(), options)


def double(**options: object) -> FieldDeclaration:
    return _declare(Double(), options)


def boolean(**options: object) -> FieldDeclaration:
    return _declare(Boolean(), options)


def date(**options: object) -> FieldDeclaration:
    return _declare(Date(), options)


def timestamp(**options: object) -> FieldDeclaration:
    return _declare(Timestamp(), options)


def decimal(precision: int, scale: int, **options: object) -> FieldDeclaration:
    return _declare(Decimal(precision, scale), options)


def array(element: FieldDeclaration, *, contains_null: bool = True, **options: object) -> FieldDeclaration:
    return _declare(Array(_nested_type(element, "array"), contains_null=contains_null), options)


def map(
    key: FieldDeclaration,
    value: FieldDeclaration,
    *,
    value_contains_null: bool = True,
    **options: object,
) -> FieldDeclaration:
    return _declare(
        Map(
            _nested_type(key, "map key"),
            _nested_type(value, "map value"),
            value_contains_null=value_contains_null,
        ),
        options,
    )


def struct(schema: type["Schema"], **options: object) -> FieldDeclaration:
    return _declare(Struct(schema), options)


def _declare(type: StructureType, options: dict[str, object]) -> FieldDeclaration:
    allowed = {"nullable", "alias", "metadata", "description"}
    unknown = set(options) - allowed
    if unknown:
        names = ", ".join(sorted(unknown))
        raise TypeError(f"field factory got unsupported option(s): {names}")
    nullable = options.get("nullable", True)
    alias = options.get("alias")
    metadata = options.get("metadata")
    description = options.get("description")
    if not isinstance(nullable, bool):
        raise TypeError("field factory nullable must be a bool")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise TypeError("field factory metadata must be a mapping or None")
    if description is not None and not isinstance(description, str):
        raise TypeError("field factory description must be a string or None")
    _require_alias(alias)  # type: ignore[arg-type]
    return FieldDeclaration(
        type,
        nullable=nullable,
        alias=cast(str | None, alias),
        metadata=metadata or {},
        description=description,
        _options=frozenset(options),
    )


def _nested_type(declaration: FieldDeclaration, context: str) -> StructureType:
    if not isinstance(declaration, FieldDeclaration):
        raise TypeError(f"field.{context}(...) requires a nested field declaration such as field.string()")
    if declaration._options:
        if "nullable" in declaration._options:
            option = "contains_null=False" if context == "array" else "value_contains_null=False"
            raise TypeError(f"field.{context}(...) controls nested nullability with {option}")
        raise TypeError(f"field.{context}(...) nested declarations may only specify a type")
    return declaration.type
