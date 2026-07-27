from __future__ import annotations

from typing import cast

from structure.dsl import Schema
from structure.plugin.api.v1.model import current_symbolic_context
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.operations import OperationPlan, PosexplodeStructPlan
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.dsl.types import ArrayType, LongType, StructType


def posexplode_struct(
    value: object,
    *,
    as_: type[Schema],
    ordinal: str = "ordinal",
    scope: str | None = None,
) -> RowScope:
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("posexplode_struct(...) can only be used inside a compiled Structure step method")
    if not isinstance(as_, type) or not issubclass(as_, Schema):
        raise TypeError("posexplode_struct(as_=...) requires a Structure Schema class")
    if not isinstance(ordinal, str) or not ordinal:
        raise TypeError("posexplode_struct(ordinal=...) requires a non-empty field name")
    if scope is not None and (not isinstance(scope, str) or not scope):
        raise TypeError("posexplode_struct(scope=...) requires a non-empty string")

    expression = literal(value)
    if not isinstance(expression, Expression) or not isinstance(expression.type, ArrayType):
        raise TypeError("posexplode_struct(...) requires an array<struct<...>> Structure expression")
    if not isinstance(expression.type.element, StructType):
        raise TypeError("posexplode_struct(...) requires an array<struct<...>> Structure expression")
    if expression.type.contains_null:
        raise TypeError("posexplode_struct(...) requires contains_null=False until null element semantics are admitted")

    element_schema = expression.type.element.schema
    _validate_generated_schema(as_, element_schema=element_schema, ordinal=ordinal)
    _validate_source_collisions(context.default_project_source, generated=as_)

    generated_scope = scope or _default_scope(as_)
    context.operations.append(
        OperationPlan.posexplode_struct_operation(
            PosexplodeStructPlan(
                expression=expression,
                scope=generated_scope,
                schema=as_,
                ordinal=ordinal,
            )
        )
    )
    context.register_current_scope(generated_scope)
    return RowScope(name=generated_scope, schema=as_)


def _validate_generated_schema(schema: type[Schema], *, element_schema: type[Schema], ordinal: str) -> None:
    fields = schema._structure_fields
    if ordinal not in fields:
        raise TypeError(f"posexplode_struct(as_=...) schema must declare ordinal field {ordinal!r}")
    if not isinstance(fields[ordinal].type, LongType):
        raise TypeError(f"posexplode_struct(ordinal={ordinal!r}) field must be long()")
    for name, field in element_schema._structure_fields.items():
        if name not in fields:
            raise TypeError(f"posexplode_struct(as_=...) schema must declare element field {name!r}")
        if fields[name].type != field.type:
            raise TypeError(f"posexplode_struct(as_=...) field {name!r} must match the array element field type")


def _validate_source_collisions(source: object, *, generated: type[Schema]) -> None:
    source_schema = getattr(source, "_structure_scope_schema", None)
    if not isinstance(source_schema, type) or not issubclass(source_schema, Schema):
        return
    source_schema = cast(type[Schema], source_schema)
    source_columns = {field.column for field in source_schema._structure_fields.values()}
    generated_columns = {field.column for field in generated._structure_fields.values()}
    collisions = sorted(source_columns & generated_columns)
    if collisions:
        raise TypeError(
            "posexplode_struct(as_=...) generated columns collide with current input column(s): "
            f"{', '.join(collisions)}. Use field aliases on the generated schema."
        )


def _default_scope(schema: type[Schema]) -> str:
    name = schema.__name__
    return name[:1].lower() + name[1:]
