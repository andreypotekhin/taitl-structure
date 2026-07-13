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
from structure.app.dsl.model.types.IntegerType import IntegerType
from structure.app.dsl.model.types.LongType import LongType
from structure.app.dsl.model.types.MapType import MapType
from structure.app.dsl.model.types.StringType import StringType
from structure.app.dsl.model.types.StructureType import StructureType

F = TypeVar("F", bound=Callable)


def group_by(*keys: object, **named_keys: object) -> "GroupedRows":
    return _grouping("group_by", "group_by(...)", keys, named_keys)


def rollup(*keys: object, **named_keys: object) -> "GroupedRows":
    return _grouping("rollup", "rollup(...)", keys, named_keys)


def cube(*keys: object, **named_keys: object) -> "GroupedRows":
    return _grouping("cube", "cube(...)", keys, named_keys)


def grouping_sets(*levels: object, **named_levels: object) -> "GroupedRows":
    context = _context("grouping_sets(...)")
    parsed_levels = tuple(_grouping_set_level(level) for level in (*levels, *named_levels.values()))
    if not parsed_levels:
        raise TypeError("grouping_sets(...) requires at least one grouping level")
    keys = _grouping_set_keys(parsed_levels)
    if not keys:
        raise TypeError("grouping_sets(...) requires at least one non-empty grouping level")
    context.aggregate_keys = keys
    context.aggregate_levels = tuple(tuple(name for name, _ in level) for level in parsed_levels)
    context.aggregate_grouping = "grouping_sets"
    return GroupedRows(keys)


def grouping_id() -> Expression:
    return _aggregate("grouping_id", type=IntegerType(), nullable=False)


def is_grouped(value: object) -> Expression:
    return _aggregate("is_grouped", literal(value), type=BooleanType(), nullable=False)


def having(predicate: object) -> None:
    context = _context("having(...)")
    if context.aggregate_having is not None:
        raise TypeError("having(...) can only be declared once per aggregate step")
    context.aggregate_having = predicate


def _grouping(kind: str, call: str, keys: tuple[object, ...], named_keys: dict[str, object]) -> "GroupedRows":
    context = _context(call)
    expressions = (*_positional_keys(keys), *_named_keys(named_keys))
    if not expressions:
        raise TypeError(f"{call} requires at least one grouping key")
    context.aggregate_keys = expressions
    context.aggregate_grouping = kind
    return GroupedRows(expressions)


def count(*, where: object | None = None) -> Expression:
    return _aggregate("count", type=LongType(), where=where)


def count_distinct(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("count_distinct", literal(value), type=LongType(), nullable=False, where=where)


def min(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate("min", argument, type=argument.type, nullable=argument.nullable, where=where)


def max(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate("max", argument, type=argument.type, nullable=argument.nullable, where=where)


def avg(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate("avg", argument, type=DoubleType(), nullable=argument.nullable, where=where)


def sum(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate("sum", argument, type=argument.type, nullable=argument.nullable, where=where)


def bool_and(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("bool_and", literal(value), type=BooleanType(), nullable=True, where=where)


def bool_or(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("bool_or", literal(value), type=BooleanType(), nullable=True, where=where)


def stddev(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("stddev", literal(value), type=DoubleType(), nullable=True, where=where)


def variance(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("variance", literal(value), type=DoubleType(), nullable=True, where=where)


def corr(left: object, right: object, *, where: object | None = None) -> Expression:
    return _aggregate("corr", literal(left), literal(right), type=DoubleType(), nullable=True, where=where)


def covar(left: object, right: object, *, where: object | None = None) -> Expression:
    return _aggregate("covar", literal(left), literal(right), type=DoubleType(), nullable=True, where=where)


def approx_count_distinct(value: object, *, relative_sd: float | None = None, where: object | None = None) -> Expression:
    return _aggregate(
        "approx_count_distinct",
        literal(value),
        type=LongType(),
        nullable=False,
        where=where,
        options=(("relative_sd", relative_sd),),
    )


def approx_percentile(
    value: object,
    percentage: float,
    *,
    accuracy: int | None = None,
    where: object | None = None,
) -> Expression:
    argument = literal(value)
    return _aggregate(
        "approx_percentile",
        argument,
        type=argument.type,
        nullable=True,
        where=where,
        options=(("percentage", percentage), ("accuracy", accuracy)),
    )


def collect_list(
    value: object, *, element_type: StructureType | None = None, where: object | None = None
) -> Expression:
    argument = literal(value)
    return _aggregate(
        "collect_list",
        argument,
        type=ArrayType(_collection_element_type(argument, element_type), contains_null=argument.nullable),
        nullable=True,
        where=where,
    )


def collect_set(
    value: object, *, element_type: StructureType | None = None, where: object | None = None
) -> Expression:
    argument = literal(value)
    return _aggregate(
        "collect_set",
        argument,
        type=ArrayType(_collection_element_type(argument, element_type), contains_null=argument.nullable),
        nullable=True,
        where=where,
    )


def first_value(
    value: object,
    *,
    order_by: object | None = None,
    over: "WindowSpec | None" = None,
    ignore_nulls: bool = False,
    where: object | None = None,
    ties: TiePolicy = TiePolicy.ERROR,
) -> Expression:
    argument = literal(value)
    if over is not None:
        return _window_over_expression("first_value", argument, over=over, type=argument.type, nullable=True, ignore_nulls=ignore_nulls)
    if order_by is None:
        raise TypeError("first_value(...) aggregate requires order_by=...")
    if ties is not TiePolicy.ERROR:
        raise TypeError("first_value(...) currently supports ties=TiePolicy.ERROR only")
    return _aggregate(
        "first_value",
        argument,
        type=argument.type,
        nullable=argument.nullable,
        where=where,
        order_by=literal(order_by),
    )


def last_value(
    value: object,
    *,
    order_by: object | None = None,
    over: "WindowSpec | None" = None,
    ignore_nulls: bool = False,
    where: object | None = None,
    ties: TiePolicy = TiePolicy.ERROR,
) -> Expression:
    argument = literal(value)
    if over is not None:
        return _window_over_expression("last_value", argument, over=over, type=argument.type, nullable=True, ignore_nulls=ignore_nulls)
    if order_by is None:
        raise TypeError("last_value(...) aggregate requires order_by=...")
    if ties is not TiePolicy.ERROR:
        raise TypeError("last_value(...) currently supports ties=TiePolicy.ERROR only")
    return _aggregate(
        "last_value",
        argument,
        type=argument.type,
        nullable=argument.nullable,
        where=where,
        order_by=literal(order_by),
    )


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


def window(*, partition_by: object, order_by: object, frame: "WindowFrame | None" = None) -> "WindowSpec":
    spec = WindowSpec(
        partition_by=_partition_by(partition_by, call="window(...)"),
        order_by=_order_by(order_by, call="window(...)"),
        frame=frame,
    )
    _validate_window_spec(spec)
    return spec


def rows_between(start: "WindowBound", end: "WindowBound") -> "WindowFrame":
    return _window_frame("rows", start, end)


def range_between(start: "WindowBound", end: "WindowBound") -> "WindowFrame":
    return _window_frame("range", start, end)


def unbounded_preceding() -> "WindowBound":
    return WindowBound("unbounded_preceding")


def unbounded_following() -> "WindowBound":
    return WindowBound("unbounded_following")


def current_row() -> "WindowBound":
    return WindowBound("current_row")


def preceding(value: int) -> "WindowBound":
    if value < 0:
        raise TypeError("preceding(...) value must be greater than or equal to 0")
    return WindowBound("preceding", value)


def following(value: int) -> "WindowBound":
    if value < 0:
        raise TypeError("following(...) value must be greater than or equal to 0")
    return WindowBound("following", value)


def percent_rank(*, over: "WindowSpec") -> Expression:
    return _window_over_expression("percent_rank", over=over, type=DoubleType(), nullable=False)


def cume_dist(*, over: "WindowSpec") -> Expression:
    return _window_over_expression("cume_dist", over=over, type=DoubleType(), nullable=False)


def ntile(value: int, *, over: "WindowSpec") -> Expression:
    if value < 1:
        raise TypeError("ntile(...) value must be greater than or equal to 1")
    return _window_over_expression("ntile", over=over, type=IntegerType(), nullable=False, options=(("buckets", value),))


def nth_value(value: object, n: int, *, over: "WindowSpec", ignore_nulls: bool = False) -> Expression:
    if n < 1:
        raise TypeError("nth_value(...) n must be greater than or equal to 1")
    argument = literal(value)
    return _window_over_expression(
        "nth_value",
        argument,
        over=over,
        type=argument.type,
        nullable=True,
        ignore_nulls=ignore_nulls,
        options=(("n", n),),
    )


def window_sum(value: object, *, over: "WindowSpec") -> Expression:
    argument = literal(value)
    return _window_over_expression("sum", argument, over=over, type=argument.type, nullable=argument.nullable)


def window_avg(value: object, *, over: "WindowSpec") -> Expression:
    return _window_over_expression("avg", literal(value), over=over, type=DoubleType(), nullable=True)


def window_min(value: object, *, over: "WindowSpec") -> Expression:
    argument = literal(value)
    return _window_over_expression("min", argument, over=over, type=argument.type, nullable=argument.nullable)


def window_max(value: object, *, over: "WindowSpec") -> Expression:
    argument = literal(value)
    return _window_over_expression("max", argument, over=over, type=argument.type, nullable=argument.nullable)


def window_count(value: object | None = None, *, over: "WindowSpec") -> Expression:
    args = () if value is None else (literal(value),)
    return _window_over_expression("count", *args, over=over, type=LongType(), nullable=False)


def window_count_distinct(value: object, *, over: "WindowSpec") -> Expression:
    raise TypeError(
        "window_count_distinct(...) is not supported because Spark does not permit distinct window aggregates; "
        "use window_count(...) or aggregate with count_distinct(...) instead"
    )


def window_bool_and(value: object, *, over: "WindowSpec") -> Expression:
    argument = _window_boolean(value, "window_bool_and(...)")
    return _window_over_expression("bool_and", argument, over=over, type=BooleanType(), nullable=True)


def window_bool_or(value: object, *, over: "WindowSpec") -> Expression:
    argument = _window_boolean(value, "window_bool_or(...)")
    return _window_over_expression("bool_or", argument, over=over, type=BooleanType(), nullable=True)


def window_stddev(value: object, *, over: "WindowSpec") -> Expression:
    argument = _window_numeric(value, "window_stddev(...)")
    return _window_over_expression("stddev", argument, over=over, type=DoubleType(), nullable=True)


def window_variance(value: object, *, over: "WindowSpec") -> Expression:
    argument = _window_numeric(value, "window_variance(...)")
    return _window_over_expression("variance", argument, over=over, type=DoubleType(), nullable=True)


def window_collect_list(
    value: object, *, over: "WindowSpec", element_type: StructureType | None = None
) -> Expression:
    argument = literal(value)
    return _window_over_expression(
        "collect_list",
        argument,
        over=over,
        type=ArrayType(_collection_element_type(argument, element_type), contains_null=argument.nullable),
        nullable=True,
    )


def window_collect_set(
    value: object, *, over: "WindowSpec", element_type: StructureType | None = None
) -> Expression:
    argument = literal(value)
    return _window_over_expression(
        "collect_set",
        argument,
        over=over,
        type=ArrayType(_collection_element_type(argument, element_type), contains_null=argument.nullable),
        nullable=True,
    )


def drop_duplicates(*subset: object) -> None:
    duplicate_rows = _duplicate_rows(subset, call="drop_duplicates(...)")
    _context("drop_duplicates()").operations.append(
        OperationPlan.drop_duplicates_operation(duplicate_rows)
    )


def distinct(relation: object | None = None) -> None:
    duplicate_rows = DuplicateRowsPlan() if relation is None else _duplicate_rows((relation,), call="distinct(...)")
    _context("distinct()").operations.append(OperationPlan.drop_duplicates_operation(duplicate_rows))


def _aggregate(
    function: str,
    *arguments: Expression,
    type,
    nullable: bool = False,
    where: object | None = None,
    order_by: Expression | None = None,
    options: tuple[tuple[str, object], ...] = (),
) -> Expression:
    args = arguments
    data: dict[str, object] = {
        "function": function,
        "capability_group": "aggregate",
        "capability_name": function,
        "arg_count": len(arguments),
    }
    if where is not None:
        data["where_index"] = len(args)
        args = (*args, literal(where))
    if order_by is not None:
        data["order_by_index"] = len(args)
        args = (*args, order_by)
    data.update({key: value for key, value in options if value is not None})
    return Expression(
        kind="aggregate",
        type=type,
        nullable=nullable,
        data=data,
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
    ordering = _order_by(order_by, call=f"{function}(...)")
    data = {
        "function": f"window_{function}",
        "capability_group": "window",
        "capability_name": function,
        "descending": descending,
        "order_count": len(ordering),
    }
    if offset is not None:
        data["offset"] = offset
        data["default"] = default
        data["has_default"] = default is not None
    args = (() if value is None else (value,)) + (*ordering, *partitions)
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
    ordering = _order_by(order_by, call=f"rolling_{function}(...)")
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
            "order_count": len(ordering),
        },
        args=(value, *ordering, *partitions),
    )


def _window_over_expression(
    function: str,
    *values: Expression,
    over: "WindowSpec",
    type,
    nullable: bool,
    ignore_nulls: bool = False,
    options: tuple[tuple[str, object], ...] = (),
) -> Expression:
    _validate_window_spec(over)
    if function in _WINDOW_AGGREGATES and over.frame is None:
        raise TypeError(
            f"window_{function}(...) requires an explicit frame such as rows_between(preceding(2), current_row())"
        )
    data: dict[str, object] = {
        "function": f"window_{function}",
        "capability_group": "window",
        "capability_name": function,
        "value_count": len(values),
        "ignore_nulls": ignore_nulls,
        "order_count": len(over.order_by),
    }
    if over.frame is not None:
        data["frame_kind"] = over.frame.kind
        data["frame_start"] = over.frame.start.as_pyspark()
        data["frame_end"] = over.frame.end.as_pyspark()
    data.update({key: value for key, value in options if value is not None})
    return Expression(
        kind="reserved_v2",
        type=type,
        nullable=nullable,
        data=data,
        args=(*values, *over.order_by, *over.partition_by),
    )


def _partition_by(partition_by: object, *, call: str) -> tuple[Expression, ...]:
    values = partition_by if isinstance(partition_by, (tuple, list)) else (partition_by,)
    partitions = tuple(literal(value) for value in values)
    if not partitions:
        raise TypeError(f"{call} requires at least one partition_by expression")
    return partitions


def _order_by(order_by: object, *, call: str) -> tuple[Expression, ...]:
    values = order_by if isinstance(order_by, (tuple, list)) else (order_by,)
    ordering = tuple(literal(value) for value in values)
    if not ordering:
        raise TypeError(f"{call} requires at least one order_by expression")
    return ordering


def _window_frame(kind: str, start: "WindowBound", end: "WindowBound") -> "WindowFrame":
    if not isinstance(start, WindowBound) or not isinstance(end, WindowBound):
        raise TypeError(f"{kind}_between(...) requires WindowBound values such as preceding(1) and current_row()")
    if _window_bound_position(start) > _window_bound_position(end):
        raise TypeError(f"{kind}_between(...) start must not be after end")
    return WindowFrame(kind=kind, start=start, end=end)


def _validate_window_spec(spec: "WindowSpec") -> None:
    if spec.frame is not None and spec.frame.kind == "range" and len(spec.order_by) != 1:
        raise TypeError("range_between(...) requires exactly one order_by expression")


def _window_bound_position(bound: "WindowBound") -> float:
    if bound.kind == "unbounded_preceding":
        return float("-inf")
    if bound.kind == "unbounded_following":
        return float("inf")
    if bound.kind == "current_row":
        return 0
    if bound.kind == "preceding":
        return -float(bound.value or 0)
    if bound.kind == "following":
        return float(bound.value or 0)
    raise TypeError(f"Unsupported window bound: {bound.kind}")


def _window_boolean(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, BooleanType):
        raise TypeError(f"{call} requires a Boolean expression")
    return argument


def _window_numeric(value: object, call: str) -> Expression:
    argument = literal(value)
    if argument.type is None or argument.type.name not in {"integer", "long", "float", "double", "decimal"}:
        raise TypeError(f"{call} requires a numeric expression")
    return argument


_WINDOW_AGGREGATES = frozenset(
    {
        "avg",
        "bool_and",
        "bool_or",
        "collect_list",
        "collect_set",
        "count",
        "max",
        "min",
        "stddev",
        "sum",
        "variance",
    }
)


def _duplicate_rows(subset: tuple[object, ...], *, call: str) -> DuplicateRowsPlan:
    if not subset:
        return DuplicateRowsPlan()
    relation = _relation_subset(subset[0]) if len(subset) == 1 else None
    if relation is not None:
        relation_scope, fields = relation
        return DuplicateRowsPlan(subset=fields, scope=relation_scope)
    fields = _dedupe_subset(subset, call=call)
    scope = _dedupe_scope(fields, call=call)
    return DuplicateRowsPlan(subset=fields, scope=scope)


def _relation_subset(value: object) -> tuple[str, tuple[Expression, ...]] | None:
    scope = getattr(value, "_structure_scope_name", None)
    schema = getattr(value, "_structure_scope_schema", None)
    fields = getattr(schema, "_structure_fields", None)
    if not isinstance(scope, str) or not isinstance(fields, dict):
        return None
    return scope, tuple(getattr(value, name) for name in fields)


def _dedupe_subset(subset: tuple[object, ...], *, call: str) -> tuple[Expression, ...]:
    values = subset[0] if len(subset) == 1 and isinstance(subset[0], (tuple, list)) else subset
    fields = tuple(literal(value) for value in values)
    for field in fields:
        if field.kind != "field":
            raise TypeError(f"{call} subset accepts field expressions such as row.id")
    return fields


def _dedupe_scope(fields: tuple[Expression, ...], *, call: str) -> str | None:
    scopes = {str(field.data["scope"]) for field in fields if field.data and "scope" in field.data}
    if len(scopes) > 1:
        raise TypeError(f"{call} accepts fields from one relation scope per call")
    return next(iter(scopes), None)


@dataclass(frozen=True)
class GroupedRows:
    keys: tuple[tuple[str, Expression], ...]

    def agg(self, **aggregates: object) -> "GroupedAggregates":
        return GroupedAggregates(self.keys, tuple((name, literal(value)) for name, value in aggregates.items()))


@dataclass(frozen=True)
class GroupedAggregates:
    keys: tuple[tuple[str, Expression], ...]
    aggregates: tuple[tuple[str, Expression], ...]
    having_predicate: object | None = None

    def having(self, predicate: object) -> "GroupedAggregates":
        if self.having_predicate is not None:
            raise TypeError("having(...) can only be declared once per aggregate step")
        return GroupedAggregates(self.keys, self.aggregates, predicate)

    def as_schema(self, schema):
        if self.having_predicate is not None:
            context = _context("having(...)")
            if context.aggregate_having is not None:
                raise TypeError("having(...) can only be declared once per aggregate step")
            context.aggregate_having = self.having_predicate
        values = {name: expression for name, expression in self.keys}
        values.update({name: expression for name, expression in self.aggregates})
        return schema(**values)


@dataclass(frozen=True)
class WindowSpec:
    partition_by: tuple[Expression, ...]
    order_by: tuple[Expression, ...]
    frame: "WindowFrame | None" = None


@dataclass(frozen=True)
class WindowFrame:
    kind: str
    start: "WindowBound"
    end: "WindowBound"


@dataclass(frozen=True)
class WindowBound:
    kind: str
    value: int | None = None

    def as_pyspark(self) -> str | int:
        if self.kind == "unbounded_preceding":
            return "Window.unboundedPreceding"
        if self.kind == "unbounded_following":
            return "Window.unboundedFollowing"
        if self.kind == "current_row":
            return "Window.currentRow"
        if self.kind == "preceding":
            return -(self.value or 0)
        if self.kind == "following":
            return self.value or 0
        raise TypeError(f"Unsupported window bound: {self.kind}")


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


def arr_exists(value: object, function: Callable[[Expression], object]) -> Expression:
    argument = literal(value)
    array = _array_type(argument, "arr_exists(...)")
    element = _lambda_arg(array.element, nullable=array.contains_null, name="item")
    predicate = _callback_expression("arr_exists(...)", function, element)
    if not isinstance(predicate.type, BooleanType):
        raise TypeError("arr_exists(...) callback must return a Boolean expression")
    return _reserved_expression(
        "array_exists",
        group="higher_order",
        name="array_exists",
        type=BooleanType(),
        nullable=argument.nullable,
        args=(argument, predicate),
    )


def arr_forall(value: object, function: Callable[[Expression], object]) -> Expression:
    argument = literal(value)
    array = _array_type(argument, "arr_forall(...)")
    element = _lambda_arg(array.element, nullable=array.contains_null, name="item")
    predicate = _callback_expression("arr_forall(...)", function, element)
    if not isinstance(predicate.type, BooleanType):
        raise TypeError("arr_forall(...) callback must return a Boolean expression")
    return _reserved_expression(
        "array_forall",
        group="higher_order",
        name="array_forall",
        type=BooleanType(),
        nullable=argument.nullable,
        args=(argument, predicate),
    )


def arr_zip_with(left: object, right: object, function: Callable[[Expression, Expression], object]) -> Expression:
    left_arg = literal(left)
    right_arg = literal(right)
    left_array = _array_type(left_arg, "arr_zip_with(...)")
    right_array = _array_type(right_arg, "arr_zip_with(...)")
    left_item = _lambda_arg(left_array.element, nullable=True, name="left_item")
    right_item = _lambda_arg(right_array.element, nullable=True, name="right_item")
    result = _callback_expression("arr_zip_with(...)", function, left_item, right_item)
    result_type = result.type
    if result_type is None:
        raise AssertionError("higher-order callback validation must reject untyped results")
    return _reserved_expression(
        "array_zip_with",
        group="higher_order",
        name="array_zip_with",
        type=ArrayType(result_type, contains_null=result.nullable),
        nullable=left_arg.nullable or right_arg.nullable,
        args=(left_arg, right_arg, left_item, right_item, result),
    )


def arr_aggregate(
    value: object,
    initial: object,
    merge: Callable[[Expression, Expression], object],
    finish: Callable[[Expression], object] | None = None,
) -> Expression:
    argument = literal(value)
    initial_value = literal(initial)
    array = _array_type(argument, "arr_aggregate(...)")
    accumulator = _lambda_arg(initial_value.type, nullable=initial_value.nullable, name="acc")
    item = _lambda_arg(array.element, nullable=array.contains_null, name="item")
    merged = _callback_expression("arr_aggregate(...)", merge, accumulator, item)
    finished = _callback_expression("arr_aggregate(...)", finish, merged) if finish is not None else merged
    result_type = finished.type
    if result_type is None:
        raise AssertionError("higher-order callback validation must reject untyped results")
    return _reserved_expression(
        "array_aggregate",
        group="higher_order",
        name="array_aggregate",
        type=result_type,
        nullable=finished.nullable,
        args=(argument, initial_value, accumulator, item, merged, finished),
    )


def arr_sort_by(value: object, function: Callable[[Expression], object], *, descending: bool = False) -> Expression:
    argument = literal(value)
    array = _array_type(argument, "arr_sort_by(...)")
    element = _lambda_arg(array.element, nullable=array.contains_null, name="item")
    _callback_expression("arr_sort_by(...)", function, element)
    return _reserved_expression(
        "array_sort_by",
        group="higher_order",
        name="array_sort_by",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument,),
        data=(("descending", descending),),
    )


def arr_flatten(value: object) -> Expression:
    argument = literal(value)
    array = _array_type(argument, "arr_flatten(...)")
    nested = array.element
    if not isinstance(nested, ArrayType):
        raise TypeError("arr_flatten(...) requires an Array of Array expression")
    return _reserved_expression(
        "array_flatten",
        group="higher_order",
        name="array_flatten",
        type=ArrayType(nested.element, contains_null=nested.contains_null),
        nullable=argument.nullable,
        args=(argument,),
    )


def arr_distinct(value: object) -> Expression:
    argument = literal(value)
    _array_type(argument, "arr_distinct(...)")
    return _reserved_expression(
        "array_distinct",
        group="higher_order",
        name="array_distinct",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument,),
    )


def arr_position(value: object, item: object) -> Expression:
    argument = literal(value)
    _array_type(argument, "arr_position(...)")
    return _reserved_expression(
        "array_position",
        group="higher_order",
        name="array_position",
        type=LongType(),
        nullable=True,
        args=(argument, literal(item)),
    )


def map_transform_keys(
    value: object,
    function: Callable[[Expression, Expression], object],
    *,
    duplicates: str = "error",
) -> Expression:
    if duplicates != "error":
        raise TypeError('map_transform_keys(...) currently supports duplicates="error" only')
    argument = literal(value)
    map_type = _map_type(argument, "map_transform_keys(...)")
    key = _lambda_arg(map_type.key, nullable=False, name="key")
    item = _lambda_arg(map_type.value, nullable=map_type.value_contains_null, name="value")
    result = _callback_expression("map_transform_keys(...)", function, key, item)
    result_type = result.type
    if result_type is None:
        raise AssertionError("higher-order callback validation must reject untyped results")
    return _reserved_expression(
        "map_transform_keys",
        group="higher_order",
        name="map_transform_keys",
        type=MapType(result_type, map_type.value, value_contains_null=map_type.value_contains_null),
        nullable=argument.nullable,
        args=(argument, key, item, result),
    )


def map_zip_with(
    left: object,
    right: object,
    function: Callable[[Expression, Expression, Expression], object],
) -> Expression:
    left_arg = literal(left)
    right_arg = literal(right)
    left_map = _map_type(left_arg, "map_zip_with(...)")
    right_map = _map_type(right_arg, "map_zip_with(...)")
    key = _lambda_arg(left_map.key, nullable=False, name="key")
    left_value = _lambda_arg(left_map.value, nullable=True, name="left_value")
    right_value = _lambda_arg(right_map.value, nullable=True, name="right_value")
    result = _callback_expression("map_zip_with(...)", function, key, left_value, right_value)
    result_type = result.type
    if result_type is None:
        raise AssertionError("higher-order callback validation must reject untyped results")
    return _reserved_expression(
        "map_zip_with",
        group="higher_order",
        name="map_zip_with",
        type=MapType(left_map.key, result_type, value_contains_null=result.nullable),
        nullable=left_arg.nullable or right_arg.nullable,
        args=(left_arg, right_arg, key, left_value, right_value, result),
    )


def map_keys(value: object) -> Expression:
    argument = literal(value)
    map_type = _map_type(argument, "map_keys(...)")
    return _reserved_expression(
        "map_keys",
        group="higher_order",
        name="map_keys",
        type=ArrayType(map_type.key, contains_null=False),
        nullable=argument.nullable,
        args=(argument,),
    )


def map_values(value: object) -> Expression:
    argument = literal(value)
    map_type = _map_type(argument, "map_values(...)")
    return _reserved_expression(
        "map_values",
        group="higher_order",
        name="map_values",
        type=ArrayType(map_type.value, contains_null=map_type.value_contains_null),
        nullable=argument.nullable,
        args=(argument,),
    )


def map_entries(value: object) -> Expression:
    argument = literal(value)
    _map_type(argument, "map_entries(...)")
    return _reserved_expression(
        "map_entries",
        group="higher_order",
        name="map_entries",
        type=ArrayType(MapType(StringType(), StringType()), contains_null=False),
        nullable=argument.nullable,
        args=(argument,),
    )


def map_from_entries(value: object) -> Expression:
    argument = literal(value)
    _array_type(argument, "map_from_entries(...)")
    return _reserved_expression(
        "map_from_entries",
        group="higher_order",
        name="map_from_entries",
        type=MapType(StringType(), StringType()),
        nullable=argument.nullable,
        args=(argument,),
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
    data: tuple[tuple[str, object], ...] = (),
) -> Expression:
    payload: dict[str, object] = {"function": function, "capability_group": group, "capability_name": name}
    for key, value in data:
        payload[key] = value
    return Expression(
        kind="reserved_v2",
        type=type,
        nullable=nullable,
        data=payload,
        args=args,
    )


def _collection_element_type(argument: Expression, element_type: StructureType | None) -> StructureType:
    if element_type is not None:
        return element_type
    if argument.type is None:
        raise TypeError("Collection aggregate element type is required when the value type is unknown")
    return argument.type


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


def _grouping_set_level(level: object) -> tuple[tuple[str, Expression], ...]:
    values = level if isinstance(level, (tuple, list)) else (level,)
    return _positional_keys(tuple(values))


def _grouping_set_keys(
    levels: tuple[tuple[tuple[str, Expression], ...], ...],
) -> tuple[tuple[str, Expression], ...]:
    keys: list[tuple[str, Expression]] = []
    for level in levels:
        for name, expression in level:
            existing = next((key for key in keys if _same_expression(key[1], expression)), None)
            if existing is not None:
                continue
            if any(key_name == name for key_name, _ in keys):
                raise TypeError(f"grouping_sets(...) uses grouping key name {name!r} for multiple expressions")
            keys.append((name, expression))
    return tuple(keys)


def _same_expression(left: Expression, right: Expression) -> bool:
    return left.kind == right.kind and left.data == right.data and left.args == right.args


def _key_name(expression: Expression) -> str:
    if expression.data and expression.data.get("name"):
        return str(expression.data["name"]).split(".")[-1]
    if expression.data and expression.data.get("field"):
        return str(expression.data["field"]).split(".")[-1]
    raise TypeError("Positional group_by(...) keys must be named Structure field expressions")


def _context(call: str) -> CompileContext:
    context = current_context()
    if context is None:
        raise RuntimeError(f"{call} can only be used inside a compiled Structure step method")
    return context
