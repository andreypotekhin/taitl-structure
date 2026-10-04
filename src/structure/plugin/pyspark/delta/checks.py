"""Symbolic Delta CHECK declarations and schema binding."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.types import BooleanType


@dataclass(frozen=True)
class Check:
    """A CHECK predicate authored once on a Structure schema."""

    predicate: Expression
    name: str | None = None


@dataclass(frozen=True)
class BoundCheck:
    name: str
    predicate: Expression
    fields: tuple[str, ...]


def check(predicate: object, *, name: str | None = None) -> Check:
    """Declare a symbolic native Delta CHECK without executing Spark."""
    expression = literal(predicate)
    if not isinstance(expression.type, BooleanType):
        raise TypeError("check(predicate) requires a Boolean Structure expression")
    if name is not None and (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)):
        raise ValueError("check(name=...) requires a non-empty SQL-safe identifier")
    return Check(expression, name)


def bind_checks(schema: type[Schema]) -> tuple[BoundCheck, ...]:
    """Resolve class-body field tokens to this schema's physical columns."""
    checks: list[BoundCheck] = []
    names: set[str] = set()
    declarations = {id(getattr(schema, field.name)): field for field in schema._structure_fields.values()}
    for sequence, declaration in enumerate(schema._structure_constraints, start=1):
        if not isinstance(declaration, Check):
            raise TypeError(f"{schema.__name__}.constraints must contain check(...) declarations")
        fields: set[str] = set()

        def resolve(expression: Expression) -> Expression:
            if expression.kind == "field_declaration":
                token = (expression.data or {}).get("declaration")
                field = declarations.get(id(token))
                if field is None or getattr(schema, field.name) is not token:
                    raise TypeError(f"{schema.__name__} CHECK references a field that does not belong to this schema")
                fields.add(field.name)
                return replace(
                    expression,
                    kind="field",
                    data={"scope": schema.__name__, "field": field.column, "name": field.name},
                )
            return replace(expression, args=tuple(resolve(argument) for argument in expression.args))

        predicate = resolve(declaration.predicate)
        if declaration.name is None:
            pieces = (schema.__name__, *(sorted(fields) if len(fields) == 1 else ()), f"check_{sequence:03d}")
            name = "__".join(re.sub(r"[^A-Za-z0-9_]", "_", piece) for piece in pieces)
        else:
            name = declaration.name
        key = name.casefold()
        if key in names:
            raise ValueError(f"{schema.__name__} declares duplicate Delta CHECK name {name!r}")
        names.add(key)
        checks.append(BoundCheck(name, predicate, tuple(sorted(fields))))
    return tuple(checks)
