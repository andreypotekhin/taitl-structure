from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from structure.app.compiler.compileability.streaming_compatibility.model.StreamingSupport import StreamingSupport
from structure.app.compiler.ir.model.OperationCardinality import OperationCardinality
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import CompileContext, current_context
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.types.DoubleType import DoubleType
from structure.app.dsl.model.types.LongType import LongType

F = TypeVar("F", bound=Callable)


def group_by(*keys: object, **named_keys: object) -> "GroupedRows":
    context = _context("group_by(...)")
    expressions = (*_positional_keys(keys), *_named_keys(named_keys))
    if not expressions:
        raise TypeError("group_by(...) requires at least one grouping key")
    context.aggregate_keys = expressions
    return GroupedRows(expressions)


def count() -> Expression:
    return _aggregate("count", type=LongType())


def count_distinct(value: object) -> Expression:
    return _aggregate("count_distinct", literal(value), type=LongType())


def min(value: object) -> Expression:
    argument = literal(value)
    return _aggregate("min", argument, type=argument.type)


def max(value: object) -> Expression:
    argument = literal(value)
    return _aggregate("max", argument, type=argument.type)


def avg(value: object) -> Expression:
    return _aggregate("avg", literal(value), type=DoubleType())


def sum(value: object) -> Expression:
    argument = literal(value)
    return _aggregate("sum", argument, type=argument.type)


def _aggregate(function: str, argument: Expression | None = None, *, type) -> Expression:
    args = () if argument is None else (argument,)
    return Expression(
        kind="aggregate",
        type=type,
        nullable=False,
        data={"function": function, "capability_group": "aggregate", "capability_name": function},
        args=args,
    )


@dataclass(frozen=True)
class GroupedRows:
    keys: tuple[tuple[str, Expression], ...]

    def agg(self, **aggregates: object) -> "GroupedAggregates":
        return GroupedAggregates(self.keys, tuple((name, literal(value)) for name, value in aggregates.items()))


@dataclass(frozen=True)
class GroupedAggregates:
    keys: tuple[tuple[str, Expression], ...]
    aggregates: tuple[tuple[str, Expression], ...]

    def as_schema(self, schema):
        values = {name: expression for name, expression in self.keys}
        values.update({name: expression for name, expression in self.aggregates})
        return schema(**values)


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
        setattr(function, "_structure_reserved_operations", (*operations, cache_operation(storage_level)))
        return function

    return decorate


def cache_operation(storage_level: object) -> OperationPlan:
    return OperationPlan.reserved_operation(
        "cache",
        group="optimization",
        name="cache",
        cardinality=OperationCardinality.ROW_PRESERVING,
        streaming=StreamingSupport.BATCH_ONLY,
    )


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


def _positional_keys(keys: tuple[object, ...]) -> tuple[tuple[str, Expression], ...]:
    return tuple((_key_name(literal(key)), literal(key)) for key in keys)


def _named_keys(keys: dict[str, object]) -> tuple[tuple[str, Expression], ...]:
    return tuple((name, literal(key)) for name, key in keys.items())


def _key_name(expression: Expression) -> str:
    if expression.data and expression.data.get("name"):
        return str(expression.data["name"]).split(".")[-1]
    if expression.data and expression.data.get("field"):
        return str(expression.data["field"]).split(".")[-1]
    raise TypeError("Positional group_by(...) keys must be named Structure field expressions")


def _context(call: str) -> CompileContext:
    context = current_context()
    if context is None:
        raise RuntimeError(f"{call} can only be used inside a compiled Structure subtransform")
    return context
