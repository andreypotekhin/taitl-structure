from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class StructureType:
    name: str


@dataclass(frozen=True)
class StringType(StructureType):
    def __init__(self) -> None:
        object.__setattr__(self, "name", "string")


@dataclass(frozen=True)
class DecimalType(StructureType):
    precision: int
    scale: int

    def __init__(self, precision: int, scale: int) -> None:
        if precision < 1:
            raise ValueError("Decimal precision must be positive")
        if scale < 0 or scale > precision:
            raise ValueError("Decimal scale must be between 0 and precision")

        object.__setattr__(self, "name", "decimal")
        object.__setattr__(self, "precision", precision)
        object.__setattr__(self, "scale", scale)


@dataclass(frozen=True)
class FieldDefinition:
    name: str
    type: StructureType
    nullable: bool = True
    primary_key: bool = False
    metadata: Mapping[str, object] = dataclass_field(default_factory=dict)
    description: str | None = None


class FieldDeclaration:

    def __init__(
        self,
        type: StructureType,
        *,
        nullable: bool = True,
        primary_key: bool = False,
        metadata: Mapping[str, object] | None = None,
        description: str | None = None,
    ) -> None:
        self.type = type
        self.nullable = False if primary_key else nullable
        self.primary_key = primary_key
        self.metadata = MappingProxyType(dict(metadata or {}))
        self.description = description
        self.name = ""

    def __set_name__(self, owner: type[Structure], name: str) -> None:
        self.name = name

    def definition(self) -> FieldDefinition:
        return FieldDefinition(
            name=self.name,
            type=self.type,
            nullable=self.nullable,
            primary_key=self.primary_key,
            metadata=self.metadata,
            description=self.description,
        )


class Structure:

    _structure_fields: dict[str, FieldDefinition] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        fields: dict[str, FieldDefinition] = {}
        for base in cls.__bases__:
            fields.update(getattr(base, "_structure_fields", {}))

        for value in cls.__dict__.values():
            if isinstance(value, FieldDeclaration):
                fields[value.name] = value.definition()

        cls._structure_fields = fields

    def __init__(self, **values: object) -> None:
        unknown = set(values) - set(self._structure_fields)
        if unknown:
            allowed = ", ".join(self._structure_fields)
            raise TypeError(
                f"{type(self).__name__} got unknown field(s): {', '.join(sorted(unknown))}. Allowed: {allowed}"
            )
        self._structure_values = dict(values)


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
