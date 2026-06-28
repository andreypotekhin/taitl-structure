from __future__ import annotations

from typing import Mapping

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


def field(
    type: StructureType,
    *,
    nullable: bool = True,
    primary_key: bool = False,
    alias: str | None = None,
    metadata: Mapping[str, object] | None = None,
    description: str | None = None,
) -> FieldDeclaration:
    _require_type(type)
    _require_alias(alias)
    return FieldDeclaration(
        type,
        nullable=nullable,
        primary_key=primary_key,
        alias=alias,
        metadata=metadata,
        description=description,
    )


def _require_type(type: StructureType) -> None:
    if not isinstance(type, StructureType):
        raise TypeError("field(...) requires an explicit Structure type object such as String()")


def _require_alias(alias: str | None) -> None:
    if alias is not None and (not isinstance(alias, str) or not alias):
        raise ValueError("field alias must be a non-empty string when supplied")
