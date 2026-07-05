from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from structure.app.compiler.compileability.streaming_compatibility.model.StreamingSupport import StreamingSupport
from structure.app.compiler.ir.model.DuplicateRowsPlan import DuplicateRowsPlan
from structure.app.compiler.ir.model.OperationCardinality import OperationCardinality
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.compiler.ir.model.SelectedRowsPlan import SelectedRowsPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import CompileContext, current_context
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.transforms.TiePolicy import TiePolicy
from structure.app.dsl.model.types.ArrayType import ArrayType
from structure.app.dsl.model.types.BooleanType import BooleanType
from structure.app.dsl.model.types.DoubleType import DoubleType
from structure.app.dsl.model.types.LongType import LongType
from structure.app.dsl.model.types.MapType import MapType

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


def latest_by(order_by: object, *, partition_by: object, ties: TiePolicy = TiePolicy.ERROR) -> None:
    _selected_rows("latest", order_by, partition_by=partition_by, ties=ties, call="latest_by(...)")


def earliest_by(order_by: object, *, partition_by: object, ties: TiePolicy = TiePolicy.ERROR) -> None:
    _selected_rows("earliest", order_by, partition_by=partition_by, ties=ties, call="earliest_by(...)")


def dedupe_latest_by(order_by: object, *, partition_by: object, ties: TiePolicy = TiePolicy.ERROR) -> None:
    _selected_rows("latest", order_by, partition_by=partition_by, ties=ties, call="dedupe_latest_by(...)")


def dedupe_earliest_by(order_by: object, *, partition_by: object, ties: TiePolicy = TiePolicy.ERROR) -> None:
    _selected_rows("earliest", order_by, partition_by=partition_by, ties=ties, call="dedupe_earliest_by(...)")


def row_number(*, partition_by: object, order_by: object, descending: bool = False) -> Expression:
    return _window_expression(
        "row_number",
        type=LongType(),
        nullable=False,
        partition_by=partition_by,
        order_by=order_by,
        descending=descending,
    )


def rank(*, partition_by: object, order_by: object, descending: bool = False) -> Expression:
    return _window_expression(
        "rank",
        type=LongType(),
        nullable=False,
        partition_by=partition_by,
        order_by=order_by,
        descending=descending,
    )


def dense_rank(*, partition_by: object, order_by: object, descending: bool = False) -> Expression:
    return _window_expression(
        "dense_rank",
        type=LongType(),
        nullable=False,
        partition_by=partition_by,
        order_by=order_by,
        descending=descending,
    )


def lag(
    value: object,
    *,
    partition_by: object,
    order_by: object,
    offset: int = 1,
    default: object = None,
    descending: bool = False,
) -> Expression:
    argument = literal(value)
    return _window_expression(
        "lag",
        argument,
        type=argument.type,
        nullable=argument.nullable or default is None,
        partition_by=partition_by,
        order_by=order_by,
        offset=offset,
        default=default,
        descending=descending,
    )


def lead(
    value: object,
    *,
    partition_by: object,
    order_by: object,
    offset: int = 1,
    default: object = None,
    descending: bool = False,
) -> Expression:
    argument = literal(value)
    return _window_expression(
        "lead",
        argument,
        type=argument.type,
        nullable=argument.nullable or default is None,
        partition_by=partition_by,
        order_by=order_by,
        offset=offset,
        default=default,
        descending=descending,
    )


def rolling_sum(
    value: object,
    *,
    partition_by: object,
    order_by: object,
    preceding: int,
    descending: bool = False,
) -> Expression:
    argument = literal(value)
    return _rolling_expression(
        "sum",
        argument,
        type=argument.type,
        nullable=argument.nullable,
        partition_by=partition_by,
        order_by=order_by,
        preceding=preceding,
        descending=descending,
    )


def rolling_avg(
    value: object,
    *,
    partition_by: object,
    order_by: object,
    preceding: int,
    descending: bool = False,
) -> Expression:
    argument = literal(value)
    return _rolling_expression(
        "avg",
        argument,
        type=DoubleType(),
        nullable=argument.nullable,
        partition_by=partition_by,
        order_by=order_by,
        preceding=preceding,
        descending=descending,
    )


def rolling_min(
    value: object,
    *,
    partition_by: object,
    order_by: object,
    preceding: int,
    descending: bool = False,
) -> Expression:
    argument = literal(value)
    return _rolling_expression(
        "min",
        argument,
        type=argument.type,
        nullable=argument.nullable,
        partition_by=partition_by,
        order_by=order_by,
        preceding=preceding,
        descending=descending,
    )


def rolling_max(
    value: object,
    *,
    partition_by: object,
    order_by: object,
    preceding: int,
    descending: bool = False,
) -> Expression:
    argument = literal(value)
    return _rolling_expression(
        "max",
        argument,
        type=argument.type,
        nullable=argument.nullable,
        partition_by=partition_by,
        order_by=order_by,
        preceding=preceding,
        descending=descending,
    )


def drop_duplicates(*subset: object) -> None:
    fields = _dedupe_subset(subset, call="drop_duplicates(...)")
    _context("drop_duplicates()").operations.append(
        OperationPlan.drop_duplicates_operation(DuplicateRowsPlan(subset=fields))
    )


def distinct() -> None:
    _context("distinct()").operations.append(OperationPlan.drop_duplicates_operation())


def _aggregate(function: str, argument: Expression | None = None, *, type, nullable: bool = False) -> Expression:
    args = () if argument is None else (argument,)
    return Expression(
        kind="aggregate",
        type=type,
        nullable=nullable,
        data={"function": function, "capability_group": "aggregate", "capability_name": function},
        args=args,
    )


def _selected_rows(direction: str, order_by: object, *, partition_by: object, ties: TiePolicy, call: str) -> None:
    if ties is not TiePolicy.ERROR:
        raise TypeError(f"{call} currently supports ties=TiePolicy.ERROR only")
    order = literal(order_by)
    partitions = _partition_by(partition_by, call=call)
    _context(call).operations.append(
        OperationPlan.selected_rows_operation(
            SelectedRowsPlan(direction=direction, order_by=order, partition_by=partitions, ties=ties)
        )
    )


def _window_expression(
    function: str,
    value: Expression | None = None,
    *,
    type,
    nullable: bool,
    partition_by: object,
    order_by: object,
    descending: bool,
    offset: int | None = None,
    default: object = None,
) -> Expression:
    if offset is not None and offset < 1:
        raise TypeError(f"{function}(...) offset must be greater than or equal to 1")
    partitions = _partition_by(partition_by, call=f"{function}(...)")
    ordering = literal(order_by)
    data = {
        "function": f"window_{function}",
        "capability_group": "window",
        "capability_name": function,
        "descending": descending,
    }
    if offset is not None:
        data["offset"] = offset
        data["default"] = default
        data["has_default"] = default is not None
    args = (() if value is None else (value,)) + (ordering, *partitions)
    return Expression(kind="reserved_v2", type=type, nullable=nullable, data=data, args=args)


def _rolling_expression(
    function: str,
    value: Expression,
    *,
    type,
    nullable: bool,
    partition_by: object,
    order_by: object,
    preceding: int,
    descending: bool,
) -> Expression:
    if preceding < 0:
        raise TypeError(f"rolling_{function}(...) preceding must be greater than or equal to 0")
    partitions = _partition_by(partition_by, call=f"rolling_{function}(...)")
    ordering = literal(order_by)
    return Expression(
        kind="reserved_v2",
        type=type,
        nullable=nullable,
        data={
            "function": f"window_rolling_{function}",
            "capability_group": "window",
            "capability_name": f"rolling_{function}",
            "descending": descending,
            "preceding": preceding,
        },
        args=(value, ordering, *partitions),
    )


def _partition_by(partition_by: object, *, call: str) -> tuple[Expression, ...]:
    values = partition_by if isinstance(partition_by, (tuple, list)) else (partition_by,)
    partitions = tuple(literal(value) for value in values)
    if not partitions:
        raise TypeError(f"{call} requires at least one partition_by expression")
    return partitions


def _dedupe_subset(subset: tuple[object, ...], *, call: str) -> tuple[Expression, ...]:
    values = subset[0] if len(subset) == 1 and isinstance(subset[0], (tuple, list)) else subset
    fields = tuple(literal(value) for value in values)
    for field in fields:
        if field.kind != "field":
            raise TypeError(f"{call} subset accepts field expressions such as row.id")
    return fields


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
    element = _lambda_arg(array.element, nullable=array.contains_null, name="item")
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
    element = _lambda_arg(array.element, nullable=array.contains_null, name="item")
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


def map_transform_values(value: object, function: Callable[[Expression, Expression], object]) -> Expression:
    argument = literal(value)
    map_type = _map_type(argument, "map_transform_values(...)")
    key = _lambda_arg(map_type.key, nullable=False, name="key")
    item = _lambda_arg(map_type.value, nullable=map_type.value_contains_null, name="value")
    result = _callback_expression("map_transform_values(...)", function, key, item)
    result_type = result.type
    if result_type is None:
        raise AssertionError("higher-order callback validation must reject untyped results")
    return _reserved_expression(
        "map_transform_values",
        group="higher_order",
        name="map_transform_values",
        type=MapType(map_type.key, result_type, value_contains_null=result.nullable),
        nullable=argument.nullable,
        args=(argument, key, item, result),
    )


def map_filter(value: object, function: Callable[[Expression, Expression], object]) -> Expression:
    argument = literal(value)
    map_type = _map_type(argument, "map_filter(...)")
    key = _lambda_arg(map_type.key, nullable=False, name="key")
    item = _lambda_arg(map_type.value, nullable=map_type.value_contains_null, name="value")
    predicate = _callback_expression("map_filter(...)", function, key, item)
    if not isinstance(predicate.type, BooleanType):
        raise TypeError("map_filter(...) callback must return a Boolean expression")
    return _reserved_expression(
        "map_filter",
        group="higher_order",
        name="map_filter",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument, key, item, predicate),
    )


def _callback_expression(call: str, function: Callable[..., object], *arguments: Expression) -> Expression:
    try:
        result = literal(function(*arguments))
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


def _map_type(expression: Expression, call: str) -> MapType:
    if not isinstance(expression.type, MapType):
        raise TypeError(f"{call} requires a Map expression")
    return expression.type


def _lambda_arg(type, *, nullable: bool, name: str) -> Expression:
    return Expression(
        kind="lambda_arg",
        type=type,
        nullable=nullable,
        data={"name": name},
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
