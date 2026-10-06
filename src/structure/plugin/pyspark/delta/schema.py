"""Explicit Delta column behavior declarations for Structure schemas."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from structure.dsl import FieldDeclaration, Schema


@dataclass(frozen=True)
class DeltaColumnDeclaration:
    kind: str
    field: FieldDeclaration | str
    value: object = None
    mode: str | None = None
    start: int | None = None
    step: int | None = None


def delta_generated(field: FieldDeclaration | str, *, as_: str) -> DeltaColumnDeclaration:
    """Declare a Delta generated column and its native SQL expression."""
    if not isinstance(as_, str) or not as_.strip():
        raise TypeError("delta_generated(as_=...) requires a non-empty SQL expression")
    return DeltaColumnDeclaration("generated", _field_token(field), as_.strip())


def delta_identity(
    field: FieldDeclaration | str,
    *,
    mode: str = "always",
    start: int = 1,
    step: int = 1,
) -> DeltaColumnDeclaration:
    """Declare a Delta identity column without inferring behavior from nullability."""
    if mode not in {"always", "by_default"}:
        raise ValueError("Delta identity mode must be 'always' or 'by_default'")
    for name, value in (("start", start), ("step", step)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"Delta identity {name} must be an integer")
    if step == 0:
        raise ValueError("Delta identity step cannot be zero")
    return DeltaColumnDeclaration("identity", _field_token(field), mode=mode, start=start, step=step)


def delta_default(field: FieldDeclaration | str, *, value: object) -> DeltaColumnDeclaration:
    """Declare a literal Delta default expected on an existing table column."""
    if not isinstance(value, (str, bool, int, float, Decimal, date, datetime, type(None))):
        raise TypeError("Delta column defaults must be immutable scalar literals")
    return DeltaColumnDeclaration("default", _field_token(field), value=value)


def resolve_delta_columns(schema: type[Schema]) -> dict[str, DeltaColumnDeclaration]:
    """Resolve schema declarations to Structure field names and reject ambiguity."""
    declarations = getattr(schema, "_structure_delta_columns", ())
    resolved: dict[str, DeltaColumnDeclaration] = {}
    for declaration in declarations:
        if not isinstance(declaration, DeltaColumnDeclaration):
            raise TypeError(f"{schema.__name__}.delta_columns must contain Delta column declarations")
        field_name = None
        if isinstance(declaration.field, str):
            field_name = declaration.field if declaration.field in schema._structure_fields else None
        else:
            field_name = next(
                (
                    name
                    for name in schema._structure_fields
                    if getattr(schema, name, None) is declaration.field
                ),
                None,
            )
        if field_name is None:
            raise TypeError(f"{schema.__name__}.delta_columns refers to a field outside that Schema")
        if field_name in resolved:
            raise TypeError(f"{schema.__name__}.delta_columns declares {field_name!r} more than once")
        if declaration.kind == "identity" and schema._structure_fields[field_name].type.name != "long":
            raise TypeError(f"Delta identity column {field_name!r} must use the Structure long type")
        resolved[field_name] = declaration
    return resolved


def _field_token(field: Any) -> FieldDeclaration | str:
    if isinstance(field, (FieldDeclaration, str)):
        return field
    raise TypeError("Delta column declarations require a Schema field token or field name")


__all__ = ["DeltaColumnDeclaration", "delta_default", "delta_generated", "delta_identity", "resolve_delta_columns"]
