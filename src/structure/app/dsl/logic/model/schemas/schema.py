from __future__ import annotations

from typing import Mapping

from structure.app.dsl.logic.model.schemas.FieldDeclaration import FieldDeclaration
from structure.app.dsl.logic.model.types.DecimalType import DecimalType
from structure.app.dsl.logic.model.types.StringType import StringType
from structure.app.dsl.logic.model.types.StructureType import StructureType


def String() -> StringType:
    return StringType()


def Decimal(precision: int, scale: int) -> DecimalType:
    return DecimalType(precision=precision, scale=scale)


def field(
    type: StructureType,
    *,
    nullable: bool = True,
    primary_key: bool = False,
    metadata: Mapping[str, object] | None = None,
    description: str | None = None,
) -> FieldDeclaration:
    if not isinstance(type, StructureType):
        raise TypeError("field(...) requires an explicit Structure type object such as String()")
    return FieldDeclaration(
        type,
        nullable=nullable,
        primary_key=primary_key,
        metadata=metadata,
        description=description,
    )
