from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import TypeVar

from structure.app.compiler.compileability.streaming_compatibility.model.StreamingSupport import StreamingSupport
from structure.app.compiler.ir.model.OperationCardinality import OperationCardinality
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import CompileContext, current_context
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.types.LongType import LongType

F = TypeVar("F", bound=Callable)


def group_by(**keys: object) -> None:
    context = _context("group_by(...)")
    for key in keys.values():
        literal(key)
    context.operations.append(
        OperationPlan.reserved_operation(
            "group_by",
            group="aggregate",
            name="group_by",
            cardinality=OperationCardinality.AGGREGATE,
            streaming=StreamingSupport.BATCH_ONLY,
        )
    )


def count() -> Expression:
    return _reserved_expression("count", group="aggregate", name="count", type=LongType(), nullable=False)


def sum(value: object) -> Expression:
    argument = literal(value)
    return _reserved_expression("sum", group="aggregate", name="sum", type=argument.type, args=(argument,))


def arr_transform(value: object, function: Callable[[Expression], object]) -> Expression:
    argument = literal(value)
    return _reserved_expression(
        "array_transform",
        group="higher_order",
        name="array_transform",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument,),
    )


def arr_filter(value: object, function: Callable[[Expression], object]) -> Expression:
    argument = literal(value)
    return _reserved_expression(
        "array_filter",
        group="higher_order",
        name="array_filter",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument,),
    )


def cache(storage_level: object) -> Callable[[F], F]:
    def decorate(function: F) -> F:
        operations = tuple(getattr(function, "_structure_reserved_operations", ()))
        operation = OperationPlan.reserved_operation(
            "cache",
            group="optimization",
            name="cache",
            cardinality=OperationCardinality.ROW_PRESERVING,
            streaming=StreamingSupport.BATCH_ONLY,
        )
        setattr(function, "_structure_reserved_operations", (*operations, operation))
        return function

    return decorate


def reserved_operations(function: Callable) -> tuple[OperationPlan, ...]:
    return tuple(getattr(function, "_structure_reserved_operations", ()))


def _reserved_expression(
    function: str,
    *,
    group: str,
    name: str,
    type=None,
    nullable: bool = True,
    args: tuple[Expression, ...] = (),
) -> Expression:
    return Expression(
        kind="reserved_v2",
        type=type,
        nullable=nullable,
        data={"function": function, "capability_group": group, "capability_name": name},
        args=args,
    )


def _context(call: str) -> CompileContext:
    context = current_context()
    if context is None:
        raise RuntimeError(f"{call} can only be used inside a compiled Structure subtransform")
    return context
