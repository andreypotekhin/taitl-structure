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
from structure.app.dsl.model.types.ArrayType import ArrayType
from structure.app.dsl.model.types.BooleanType import BooleanType
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
    return _aggregate("count_distinct", literal(value), type=LongType(), nullable=False)


def min(value: object) -> Expression:
    argument = literal(value)
    return _aggregate("min", argument, type=argument.type, nullable=argument.nullable)


def max(value: object) -> Expression:
    argument = literal(value)
    return _aggregate("max", argument, type=argument.type, nullable=argument.nullable)


def avg(value: object) -> Expression:
    argument = literal(value)
    return _aggregate("avg", argument, type=DoubleType(), nullable=argument.nullable)


def sum(value: object) -> Expression:
    argument = literal(value)
    return _aggregate("sum", argument, type=argument.type, nullable=argument.nullable)


def _aggregate(function: str, argument: Expression | None = None, *, type, nullable: bool = False) -> Expression:
    args = () if argument is None else (argument,)
    return Expression(
        kind="aggregate",
        type=type,
        nullable=nullable,
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
    array = _array_type(argument, "arr_transform(...)")
    element = _lambda_arg(array)
    result = _callback_expression("arr_transform(...)", function, element)
    result_type = result.type
    if result_type is None:
        raise AssertionError("higher-order callback validation must reject untyped results")
    return _reserved_expression(
        "array_transform",
        group="higher_order",
        name="array_transform",
        type=ArrayType(result_type, contains_null=result.nullable),
        nullable=argument.nullable,
        args=(argument, result),
    )


def arr_filter(value: object, function: Callable[[Expression], object]) -> Expression:
    argument = literal(value)
    array = _array_type(argument, "arr_filter(...)")
    element = _lambda_arg(array)
    predicate = _callback_expression("arr_filter(...)", function, element)
    if not isinstance(predicate.type, BooleanType):
        raise TypeError("arr_filter(...) callback must return a Boolean expression")
    return _reserved_expression(
        "array_filter",
        group="higher_order",
        name="array_filter",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument, predicate),
    )


def _callback_expression(call: str, function: Callable[[Expression], object], element: Expression) -> Expression:
    try:
        result = literal(function(element))
    except Exception as error:
        raise TypeError(
            f"{call} callback must stay inside Structure expression helpers; "
            f"unsupported Python callback code failed with {type(error).__name__}: {error}"
        ) from error
    if result.type is None:
        raise TypeError(f"{call} callback must return a typed Structure expression or literal")
    return result


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


def _array_type(expression: Expression, call: str) -> ArrayType:
    if not isinstance(expression.type, ArrayType):
        raise TypeError(f"{call} requires an Array expression")
    return expression.type


def _lambda_arg(type: ArrayType) -> Expression:
    return Expression(
        kind="lambda_arg",
        type=type.element,
        nullable=type.contains_null,
        data={"name": "item"},
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
