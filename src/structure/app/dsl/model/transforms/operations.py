from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import cache as cached
from math import isfinite
from re import fullmatch
from typing import TypeVar, overload

from structure.app.compiler.ir.model.CachePlan import CachePlan
from structure.app.compiler.ir.model.DuplicateRowsPlan import DuplicateRowsPlan
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.compiler.ir.model.SelectedRowsPlan import SelectedRowsPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import CompileContext, current_context
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.schemas.FieldDeclaration import FieldDeclaration
from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.dsl.model.transforms.TiePolicy import TiePolicy
from structure.app.dsl.model.transforms.TimeWindow import TimeWindow
from structure.app.dsl.model.types.ArrayType import ArrayType
from structure.app.dsl.model.types.BooleanType import BooleanType
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.DoubleType import DoubleType
from structure.app.dsl.model.types.FloatType import FloatType
from structure.app.dsl.model.types.IntegerType import IntegerType
from structure.app.dsl.model.types.LongType import LongType
from structure.app.dsl.model.types.MapType import MapType
from structure.app.dsl.model.types.StringType import StringType
from structure.app.dsl.model.types.StructType import StructType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.dsl.model.types.TimestampType import TimestampType

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
    context.aggregate_keys = keys
    context.aggregate_levels = tuple(tuple(name for name, _ in level) for level in parsed_levels)
    context.aggregate_grouping = "grouping_sets"
    return GroupedRows()


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
    return GroupedRows()


def count(*, where: object | None = None) -> Expression:
    return _aggregate("count", type=LongType(), where=where)


def count_distinct(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("count_distinct", literal(value), type=LongType(), nullable=False, where=where)


def min(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate("min", argument, type=argument.type, nullable=argument.nullable or where is not None, where=where)


def max(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate("max", argument, type=argument.type, nullable=argument.nullable or where is not None, where=where)


def avg(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate(
        "avg",
        argument,
        type=_avg_type(argument),
        nullable=argument.nullable or where is not None,
        where=where,
    )


def sum(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate(
        "sum", argument, type=_sum_type(argument), nullable=argument.nullable or where is not None, where=where
    )


def bool_and(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate(
        "bool_and", argument, type=BooleanType(), nullable=argument.nullable or where is not None, where=where
    )


def bool_or(value: object, *, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate(
        "bool_or", argument, type=BooleanType(), nullable=argument.nullable or where is not None, where=where
    )


def stddev(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("stddev", literal(value), type=DoubleType(), nullable=True, where=where)


def variance(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("variance", literal(value), type=DoubleType(), nullable=True, where=where)


def skewness(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("skewness", literal(value), type=DoubleType(), nullable=True, where=where)


def kurtosis(value: object, *, where: object | None = None) -> Expression:
    return _aggregate("kurtosis", literal(value), type=DoubleType(), nullable=True, where=where)


def corr(left: object, right: object, *, where: object | None = None) -> Expression:
    return _aggregate("corr", literal(left), literal(right), type=DoubleType(), nullable=True, where=where)


def covar(left: object, right: object, *, where: object | None = None) -> Expression:
    return _aggregate("covar", literal(left), literal(right), type=DoubleType(), nullable=True, where=where)


def approx_count_distinct(
    value: object, *, relative_sd: float | None = None, where: object | None = None
) -> Expression:
    if relative_sd is not None and not _relative_standard_deviation(relative_sd):
        raise TypeError(
            "approx_count_distinct(...) relative_sd must be a finite number greater than 0 and at most 0.39"
        )
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
    if not _percentage(percentage):
        raise TypeError("approx_percentile(...) percentage must be a finite number from 0 through 1")
    if accuracy is not None and not _positive_integer(accuracy):
        raise TypeError("approx_percentile(...) accuracy must be a positive integer")
    argument = literal(value)
    return _aggregate(
        "approx_percentile",
        argument,
        type=argument.type,
        nullable=True,
        where=where,
        options=(("percentage", percentage), ("accuracy", accuracy)),
    )


def percentile(value: object, percentage: float, *, frequency: int = 1, where: object | None = None) -> Expression:
    if not _percentage(percentage):
        raise TypeError("percentile(...) percentage must be a finite number from 0 through 1")
    if not _positive_integer(frequency):
        raise TypeError("percentile(...) frequency must be a positive integer")
    return _aggregate(
        "percentile",
        literal(value),
        type=DoubleType(),
        nullable=True,
        where=where,
        options=(("percentage", percentage), ("frequency", frequency)),
    )


def collect_list(
    value: object, *, element_type: StructureType | None = None, where: object | None = None
) -> Expression:
    argument = literal(value)
    return _aggregate(
        "collect_list",
        argument,
        type=ArrayType(_collection_element_type(argument, element_type), contains_null=False),
        nullable=False,
        where=where,
    )


def collect_set(value: object, *, element_type: StructureType | None = None, where: object | None = None) -> Expression:
    argument = literal(value)
    return _aggregate(
        "collect_set",
        argument,
        type=ArrayType(_collection_element_type(argument, element_type), contains_null=False),
        nullable=False,
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
    _boolean_option("first_value(...)", "ignore_nulls", ignore_nulls)
    if over is not None:
        return _window_over_expression(
            "first_value", argument, over=over, type=argument.type, nullable=True, ignore_nulls=ignore_nulls
        )
    if ignore_nulls:
        raise TypeError("first_value(..., ignore_nulls=True) requires over=...")
    if order_by is None:
        raise TypeError("first_value(...) aggregate requires order_by=...")
    if ties is not TiePolicy.ERROR:
        raise TypeError("first_value(...) currently supports ties=TiePolicy.ERROR only")
    return _aggregate(
        "first_value",
        argument,
        type=argument.type,
        nullable=argument.nullable or where is not None,
        where=where,
        order_by=_orderable_expression(order_by, "first_value(...) order_by"),
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
    _boolean_option("last_value(...)", "ignore_nulls", ignore_nulls)
    if over is not None:
        return _window_over_expression(
            "last_value", argument, over=over, type=argument.type, nullable=True, ignore_nulls=ignore_nulls
        )
    if ignore_nulls:
        raise TypeError("last_value(..., ignore_nulls=True) requires over=...")
    if order_by is None:
        raise TypeError("last_value(...) aggregate requires order_by=...")
    if ties is not TiePolicy.ERROR:
        raise TypeError("last_value(...) currently supports ties=TiePolicy.ERROR only")
    return _aggregate(
        "last_value",
        argument,
        type=argument.type,
        nullable=argument.nullable or where is not None,
        where=where,
        order_by=_orderable_expression(order_by, "last_value(...) order_by"),
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
    _integer_at_least("lag(...) offset", offset, 1)
    _window_default("lag(...)", argument, default)
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
    _integer_at_least("lead(...) offset", offset, 1)
    _window_default("lead(...)", argument, default)
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
    argument = _numeric_expression(value, "rolling_sum(...)")
    _integer_at_least("rolling_sum(...) preceding", preceding, 0)
    return _rolling_expression(
        "sum",
        argument,
        type=_sum_type(argument),
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
    argument = _numeric_expression(value, "rolling_avg(...)")
    _integer_at_least("rolling_avg(...) preceding", preceding, 0)
    return _rolling_expression(
        "avg",
        argument,
        type=_avg_type(argument),
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
    argument = _orderable_expression(value, "rolling_min(...)")
    _integer_at_least("rolling_min(...) preceding", preceding, 0)
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
    argument = _orderable_expression(value, "rolling_max(...)")
    _integer_at_least("rolling_max(...) preceding", preceding, 0)
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


@overload
def window(
    *,
    partition_by: object,
    order_by: object,
    frame: "WindowFrame | None" = None,
) -> "WindowSpec": ...


@overload
def window(
    event_time: object,
    duration: str,
    /,
    slide: str | None = None,
    start: str | None = None,
) -> Expression: ...


def window(
    *arguments: object,
    partition_by: object | None = None,
    order_by: object | None = None,
    frame: "WindowFrame | None" = None,
    slide: str | None = None,
    start: str | None = None,
) -> "WindowSpec | Expression":
    if arguments:
        if partition_by is not None or order_by is not None or frame is not None:
            raise TypeError("window(...) cannot mix event-time arguments with partition_by=, order_by=, or frame=")
        if not 2 <= len(arguments) <= 4:
            raise TypeError("window(event_time, duration, slide=None, start=None) requires event_time and duration")
        if len(arguments) >= 3 and slide is not None:
            raise TypeError("window(...) received slide both positionally and by keyword")
        if len(arguments) == 4 and start is not None:
            raise TypeError("window(...) received start both positionally and by keyword")
        event_time = literal(arguments[0])
        if not isinstance(event_time.type, TimestampType):
            raise TypeError("window(event_time, ...) requires a timestamp expression")
        duration = _positive_interval("window(... duration)", arguments[1])
        positional_slide = arguments[2] if len(arguments) >= 3 else slide
        positional_start = arguments[3] if len(arguments) == 4 else start
        resolved_slide = None if positional_slide is None else _positive_interval("window(... slide)", positional_slide)
        resolved_start = None if positional_start is None else _positive_interval("window(... start)", positional_start)
        return Expression(
            kind="time_window",
            type=StructType(TimeWindow),
            nullable=event_time.nullable,
            data={"duration": duration, "slide": resolved_slide, "start": resolved_start},
            args=(event_time,),
        )
    if slide is not None or start is not None:
        raise TypeError("window(slide=... or start=...) requires event_time and duration positional arguments")
    if partition_by is None or order_by is None:
        raise TypeError("window(partition_by=..., order_by=..., frame=None) requires partition_by and order_by")
    if frame is not None and not isinstance(frame, WindowFrame):
        raise TypeError("window(frame=...) requires rows_between(...) or range_between(...)")
    spec = WindowSpec(
        partition_by=_partition_by(partition_by, call="window(...)"),
        order_by=_order_by(order_by, call="window(...)"),
        frame=frame,
    )
    _validate_window_spec(spec)
    return spec


def session_window(event_time: object, gap: str) -> Expression:
    timestamp = literal(event_time)
    if not isinstance(timestamp.type, TimestampType):
        raise TypeError("session_window(...) requires a timestamp expression")
    return _reserved_expression(
        "session_window",
        group="streaming",
        name="session_window",
        type=StructType(TimeWindow),
        nullable=timestamp.nullable,
        args=(timestamp,),
        data=(("gap", _positive_interval("session_window(... gap)", gap)),),
    )


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
    _integer_at_least("preceding(...) value", value, 0)
    return WindowBound("preceding", value)


def following(value: int) -> "WindowBound":
    _integer_at_least("following(...) value", value, 0)
    return WindowBound("following", value)


def percent_rank(*, over: "WindowSpec") -> Expression:
    return _window_over_expression("percent_rank", over=over, type=DoubleType(), nullable=False)


def cume_dist(*, over: "WindowSpec") -> Expression:
    return _window_over_expression("cume_dist", over=over, type=DoubleType(), nullable=False)


def ntile(value: int, *, over: "WindowSpec") -> Expression:
    _integer_at_least("ntile(...) value", value, 1)
    return _window_over_expression(
        "ntile", over=over, type=IntegerType(), nullable=False, options=(("buckets", value),)
    )


def nth_value(value: object, n: int, *, over: "WindowSpec", ignore_nulls: bool = False) -> Expression:
    _integer_at_least("nth_value(...) n", n, 1)
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
    argument = _numeric_expression(value, "window_sum(...)")
    return _window_over_expression(
        "sum",
        argument,
        over=over,
        type=_sum_type(argument),
        nullable=argument.nullable or not _window_frame_includes_current_row(over),
    )


def window_avg(value: object, *, over: "WindowSpec") -> Expression:
    argument = _numeric_expression(value, "window_avg(...)")
    return _window_over_expression(
        "avg",
        argument,
        over=over,
        type=_avg_type(argument),
        nullable=argument.nullable or not _window_frame_includes_current_row(over),
    )


def window_min(value: object, *, over: "WindowSpec") -> Expression:
    argument = _orderable_expression(value, "window_min(...)")
    return _window_over_expression(
        "min",
        argument,
        over=over,
        type=argument.type,
        nullable=argument.nullable or not _window_frame_includes_current_row(over),
    )


def window_max(value: object, *, over: "WindowSpec") -> Expression:
    argument = _orderable_expression(value, "window_max(...)")
    return _window_over_expression(
        "max",
        argument,
        over=over,
        type=argument.type,
        nullable=argument.nullable or not _window_frame_includes_current_row(over),
    )


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
    return _window_over_expression(
        "bool_and",
        argument,
        over=over,
        type=BooleanType(),
        nullable=argument.nullable or not _window_frame_includes_current_row(over),
    )


def window_bool_or(value: object, *, over: "WindowSpec") -> Expression:
    argument = _window_boolean(value, "window_bool_or(...)")
    return _window_over_expression(
        "bool_or",
        argument,
        over=over,
        type=BooleanType(),
        nullable=argument.nullable or not _window_frame_includes_current_row(over),
    )


def window_stddev(value: object, *, over: "WindowSpec") -> Expression:
    argument = _window_numeric(value, "window_stddev(...)")
    return _window_over_expression("stddev", argument, over=over, type=DoubleType(), nullable=True)


def window_variance(value: object, *, over: "WindowSpec") -> Expression:
    argument = _window_numeric(value, "window_variance(...)")
    return _window_over_expression("variance", argument, over=over, type=DoubleType(), nullable=True)


def window_collect_list(value: object, *, over: "WindowSpec", element_type: StructureType | None = None) -> Expression:
    argument = literal(value)
    return _window_over_expression(
        "collect_list",
        argument,
        over=over,
        type=ArrayType(_collection_element_type(argument, element_type), contains_null=False),
        nullable=False,
    )


def window_collect_set(value: object, *, over: "WindowSpec", element_type: StructureType | None = None) -> Expression:
    argument = literal(value)
    return _window_over_expression(
        "collect_set",
        argument,
        over=over,
        type=ArrayType(_collection_element_type(argument, element_type), contains_null=False),
        nullable=False,
    )


def drop_duplicates(*subset: object) -> None:
    duplicate_rows = _duplicate_rows(subset, call="drop_duplicates(...)")
    _context("drop_duplicates()").operations.append(OperationPlan.drop_duplicates_operation(duplicate_rows))


def drop_duplicates_within_watermark(*subset: object) -> None:
    duplicate_rows = _duplicate_rows(subset, call="drop_duplicates_within_watermark(...)")
    _context("drop_duplicates_within_watermark()").operations.append(
        OperationPlan.drop_duplicates_operation(
            DuplicateRowsPlan(
                subset=duplicate_rows.subset,
                scope=duplicate_rows.scope,
                within_watermark=True,
            )
        )
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
    if (
        function in {"avg", "bool_and", "bool_or", "first_value", "last_value", "max", "min", "sum"}
        and _global_aggregate_may_be_empty()
    ):
        nullable = True
    args = arguments
    data: dict[str, object] = {
        "function": function,
        "capability_group": "aggregate",
        "capability_name": function,
        "arg_count": len(arguments),
    }
    if where is not None:
        predicate = literal(where)
        if not isinstance(predicate.type, BooleanType):
            raise TypeError(f"{function}(...) where must be a Boolean expression")
        data["where_index"] = len(args)
        args = (*args, predicate)
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


def _global_aggregate_may_be_empty() -> bool:
    context = current_context()
    return context is not None and context.aggregate_grouping == "grouping_sets" and () in context.aggregate_levels


def _selected_rows(direction: str, order_by: object, *, partition_by: object, ties: TiePolicy, call: str) -> None:
    if ties is not TiePolicy.ERROR:
        raise TypeError(f"{call} currently supports ties=TiePolicy.ERROR only")
    order = _orderable_expression(order_by, f"{call} order_by")
    if order.kind == "order":
        raise TypeError(f"{call} order_by must be an unordered expression; {direction}_by(...) selects the direction")
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
    _boolean_option(f"{function}(...)", "descending", descending)
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
    return Expression(kind="transform_expression", type=type, nullable=nullable, data=data, args=args)


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
    _boolean_option(f"rolling_{function}(...)", "descending", descending)
    if preceding < 0:
        raise TypeError(f"rolling_{function}(...) preceding must be greater than or equal to 0")
    partitions = _partition_by(partition_by, call=f"rolling_{function}(...)")
    ordering = _order_by(order_by, call=f"rolling_{function}(...)")
    return Expression(
        kind="transform_expression",
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
    _boolean_option(f"window_{function}(...)", "ignore_nulls", ignore_nulls)
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
        kind="transform_expression",
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
    ordering = tuple(_orderable_expression(value, f"{call} order_by") for value in values)
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
    frame = spec.frame
    if frame is None or frame.kind != "range":
        return
    if frame.start.kind == "unbounded_preceding" and frame.end.kind == "unbounded_following":
        return
    if len(spec.order_by) != 1:
        raise TypeError("bounded range_between(...) requires exactly one order_by expression")
    if not isinstance(spec.order_by[0].type, (DecimalType, DoubleType, FloatType, IntegerType, LongType)):
        raise TypeError("bounded range_between(...) requires a numeric order_by expression")


def _window_frame_includes_current_row(spec: "WindowSpec") -> bool:
    frame = spec.frame
    return frame is not None and _window_bound_position(frame.start) <= 0 <= _window_bound_position(frame.end)


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


def _integer_at_least(name: str, value: object, minimum: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        requirement = "a positive integer" if minimum == 1 else f"an integer greater than or equal to {minimum}"
        raise TypeError(f"{name} must be {requirement}")


def _boolean_option(call: str, name: str, value: object) -> None:
    if not isinstance(value, bool):
        raise TypeError(f"{call} {name} must be a Boolean")


def _window_default(call: str, value: Expression, default: object) -> None:
    if default is None:
        return
    if not isinstance(default, (bool, int, float, str, Decimal, date, datetime)):
        raise TypeError(f"{call} default must be a Python scalar literal or None")
    default_type = _typed_type(f"{call} default", literal(default))
    value_type = _typed_type(f"{call} value", value)
    numeric = {"decimal", "double", "float", "integer", "long"}
    if not _same_type(value_type, default_type) and not {value_type.name, default_type.name} <= numeric:
        raise TypeError(f"{call} default must be compatible with the value expression type")


def _positive_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _relative_standard_deviation(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and 0 < value <= 0.39


def _percentage(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and 0 <= value <= 1


def _window_boolean(value: object, call: str) -> Expression:
    argument = literal(value)
    if not isinstance(argument.type, BooleanType):
        raise TypeError(f"{call} requires a Boolean expression")
    return argument


def _window_numeric(value: object, call: str) -> Expression:
    return _numeric_expression(value, call)


def _numeric_expression(value: object, call: str) -> Expression:
    argument = literal(value)
    if argument.type is None or argument.type.name not in {"integer", "long", "float", "double", "decimal"}:
        raise TypeError(f"{call} requires a numeric expression")
    return argument


def _sum_type(argument: Expression) -> StructureType | None:
    type = argument.type
    if isinstance(type, DecimalType):
        return DecimalType(38 if type.precision > 28 else type.precision + 10, type.scale)
    if isinstance(type, (IntegerType, LongType)):
        return LongType()
    if isinstance(type, (FloatType, DoubleType)):
        return DoubleType()
    return type


def _avg_type(argument: Expression) -> StructureType:
    type = argument.type
    if isinstance(type, DecimalType):
        return DecimalType(
            38 if type.precision > 34 else type.precision + 4,
            38 if type.scale > 34 else type.scale + 4,
        )
    return DoubleType()


def _orderable_expression(value: object, call: str) -> Expression:
    argument = literal(value)
    if argument.type is None or argument.type.name not in {
        "date",
        "decimal",
        "double",
        "float",
        "integer",
        "long",
        "string",
        "timestamp",
    }:
        raise TypeError(f"{call} requires an orderable scalar expression")
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
    def having(self, predicate: object) -> None:
        having(predicate)


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
        nullable=argument.nullable or array.contains_null or predicate.nullable,
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
        nullable=argument.nullable or array.contains_null or predicate.nullable,
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
    accumulator_type = _typed_type("arr_aggregate(...) initial", initial_value)
    accumulator = _lambda_arg(accumulator_type, nullable=initial_value.nullable, name="acc")
    item = _lambda_arg(array.element, nullable=array.contains_null, name="item")
    merged = _callback_expression("arr_aggregate(...)", merge, accumulator, item)
    merged_type = _typed_type("arr_aggregate(...) merge", merged)
    if not _same_type(accumulator_type, merged_type):
        raise TypeError("arr_aggregate(...) merge callback must return the initial accumulator type")
    # A merge callback may turn an initially required accumulator into null.  The
    # finish callback receives that final accumulator, not merely the initial
    # value, so its expression contract must retain both nullability sources.
    finish_accumulator = _lambda_arg(
        accumulator_type,
        nullable=initial_value.nullable or merged.nullable,
        name="acc",
    )
    finished = _callback_expression("arr_aggregate(...)", finish, finish_accumulator) if finish is not None else merged
    result_type = finished.type
    if result_type is None:
        raise AssertionError("higher-order callback validation must reject untyped results")
    result_nullable = argument.nullable or finished.nullable
    if finish is None:
        # Spark returns the initial accumulator unchanged for an empty array.
        result_nullable = result_nullable or initial_value.nullable
    return _reserved_expression(
        "array_aggregate",
        group="higher_order",
        name="array_aggregate",
        type=result_type,
        nullable=result_nullable,
        args=(argument, initial_value, accumulator, item, merged, finished),
    )


def arr_sort_by(value: object, function: Callable[[Expression], object], *, descending: bool = False) -> Expression:
    _boolean_option("arr_sort_by(...)", "descending", descending)
    argument = literal(value)
    array = _array_type(argument, "arr_sort_by(...)")
    left = _lambda_arg(array.element, nullable=array.contains_null, name="left")
    right = _lambda_arg(array.element, nullable=array.contains_null, name="right")
    left_key = _callback_expression("arr_sort_by(...)", function, left)
    right_key = _callback_expression("arr_sort_by(...)", function, right)
    _sortable_type("arr_sort_by(...) callback", left_key)
    _unify_types(
        "arr_sort_by(...) callback",
        (_typed_type("arr_sort_by(...) callback", left_key), _typed_type("arr_sort_by(...) callback", right_key)),
    )
    return _reserved_expression(
        "array_sort_by",
        group="higher_order",
        name="array_sort_by",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument, left_key, right_key),
        data=(("descending", descending),),
    )


def arr_sort(value: object) -> Expression:
    argument = literal(value)
    array = _array_type(argument, "arr_sort(...)")
    _sortable_type("arr_sort(...) array element", _lambda_arg(array.element, nullable=array.contains_null, name="item"))
    return _reserved_expression(
        "array_sort",
        group="higher_order",
        name="array_sort",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument,),
    )


def arr_reverse(value: object) -> Expression:
    argument = literal(value)
    _array_type(argument, "arr_reverse(...)")
    return _reserved_expression(
        "array_reverse",
        group="higher_order",
        name="array_reverse",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument,),
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
        # Spark returns null when any immediate nested array is null, even if
        # the outer array itself is present.
        nullable=argument.nullable or array.contains_null,
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
    array_type = _array_type(argument, "arr_position(...)")
    needle = literal(item)
    if needle.kind != "literal":
        raise TypeError("arr_position(...) item must be a Python literal for PySpark 3.5 compatibility")
    _unify_types("arr_position(...)", (array_type.element, _typed_type("arr_position(...)", needle)))
    return _reserved_expression(
        "array_position",
        group="higher_order",
        name="array_position",
        type=LongType(),
        nullable=argument.nullable,
        args=(argument, needle),
    )


def size(value: object) -> Expression:
    argument = literal(value)
    _collection_type(argument, "size(...)")
    return _reserved_expression(
        "collection_size",
        group="higher_order",
        name="collection_size",
        type=IntegerType(),
        nullable=argument.nullable,
        args=(argument,),
    )


def array_contains(value: object, item: object) -> Expression:
    argument = literal(value)
    array_type = _array_type(argument, "array_contains(...)")
    needle = literal(item)
    _unify_types("array_contains(...)", (array_type.element, _typed_type("array_contains(...)", needle)))
    return _reserved_expression(
        "array_contains",
        group="higher_order",
        name="array_contains",
        type=BooleanType(),
        nullable=argument.nullable or array_type.contains_null or needle.nullable,
        args=(argument, needle),
    )


def map_contains_key(value: object, key: object) -> Expression:
    argument = literal(value)
    map_type = _map_type(argument, "map_contains_key(...)")
    key_expression = literal(key)
    if key_expression.kind != "literal":
        raise TypeError("map_contains_key(...) key must be a Python literal for PySpark 3.5 compatibility")
    _map_key_type("map_contains_key(...)", map_type, key_expression)
    return _reserved_expression(
        "map_contains_key",
        group="higher_order",
        name="map_contains_key",
        type=BooleanType(),
        nullable=argument.nullable,
        args=(argument, key_expression),
    )


def array(*values: object) -> Expression:
    if not values:
        raise TypeError("array(...) requires at least one typed value")
    arguments = tuple(literal(value) for value in values)
    element_type = _unified_argument_type("array(...)", arguments)
    return _reserved_expression(
        "array",
        group="higher_order",
        name="array",
        type=ArrayType(element_type, contains_null=any(argument.nullable for argument in arguments)),
        nullable=False,
        args=arguments,
    )


def array_repeat(value: object, count: object) -> Expression:
    item = literal(value)
    item_type = _typed_type("array_repeat(...)", item)
    repeats = literal(count)
    if not isinstance(repeats.type, (IntegerType, LongType)):
        raise TypeError("array_repeat(...) count must be an integral Structure expression")
    return _reserved_expression(
        "array_repeat",
        group="higher_order",
        name="array_repeat",
        type=ArrayType(item_type, contains_null=item.nullable),
        nullable=repeats.nullable,
        args=(item, repeats),
    )


def sequence(start: object, stop: object, step: object | None = None) -> Expression:
    begin = literal(start)
    end = literal(stop)
    arguments = (begin, end) if step is None else (begin, end, literal(step))
    element_type = _unified_argument_type("sequence(...)", arguments)
    if not isinstance(element_type, (IntegerType, LongType)):
        raise TypeError("sequence(...) requires compatible integer or long values")
    if len(arguments) == 3:
        increment = arguments[2]
        if increment.kind == "literal" and isinstance(increment.data, dict) and increment.data.get("value") == 0:
            raise TypeError("sequence(...) step must not be zero")
    return _reserved_expression(
        "array_sequence",
        group="higher_order",
        name="array_sequence",
        type=ArrayType(element_type, contains_null=False),
        nullable=any(argument.nullable for argument in arguments),
        args=arguments,
    )


def arr_append(value: object, item: object) -> Expression:
    return _array_mutation("array_append", value, item)


def arr_prepend(value: object, item: object) -> Expression:
    return _array_mutation("array_prepend", value, item)


def arr_insert(value: object, position: object, item: object) -> Expression:
    argument = literal(value)
    array_type = _array_type(argument, "arr_insert(...)")
    offset = literal(position)
    if offset.kind != "literal" or not isinstance(offset.type, (IntegerType, LongType)):
        raise TypeError("arr_insert(...) position must be an integral Python literal for PySpark 3.5 compatibility")
    if isinstance(offset.data, dict) and offset.data.get("value") == 0:
        raise TypeError("arr_insert(...) position is one-based and cannot be zero")
    element = literal(item)
    element_type = _unify_types("arr_insert(...)", (array_type.element, _typed_type("arr_insert(...)", element)))
    return _reserved_expression(
        "array_insert",
        group="higher_order",
        name="array_insert",
        type=ArrayType(element_type, contains_null=array_type.contains_null or element.nullable),
        nullable=argument.nullable,
        args=(argument, offset, element),
    )


def arr_remove(value: object, item: object) -> Expression:
    argument = literal(value)
    array_type = _array_type(argument, "arr_remove(...)")
    element = literal(item)
    if element.kind != "literal" or element.nullable:
        raise TypeError("arr_remove(...) item must be a non-null Python literal for PySpark 3.5 compatibility")
    _unify_types("arr_remove(...)", (array_type.element, _typed_type("arr_remove(...)", element)))
    return _reserved_expression(
        "array_remove",
        group="higher_order",
        name="array_remove",
        type=argument.type,
        nullable=argument.nullable,
        args=(argument, element),
    )


def arr_compact(value: object) -> Expression:
    argument = literal(value)
    array_type = _array_type(argument, "arr_compact(...)")
    return _reserved_expression(
        "array_compact",
        group="higher_order",
        name="array_compact",
        type=ArrayType(array_type.element, contains_null=False),
        nullable=argument.nullable,
        args=(argument,),
    )


def array_union(left: object, right: object) -> Expression:
    return _array_set_operation("array_union", left, right)


def array_except(left: object, right: object) -> Expression:
    return _array_set_operation("array_except", left, right)


def array_intersect(left: object, right: object) -> Expression:
    return _array_set_operation("array_intersect", left, right)


def slice(value: object, start: object, length: object) -> Expression:
    argument = literal(value)
    array = _array_type(argument, "slice(...)")
    offset = literal(start)
    size = literal(length)
    if not isinstance(offset.type, (IntegerType, LongType)) or not isinstance(size.type, (IntegerType, LongType)):
        raise TypeError("slice(...) start and length must be integral Structure expressions")
    if size.kind == "literal" and isinstance(size.data, dict) and size.data.get("value", 0) < 0:
        raise TypeError("slice(...) length must not be negative")
    return _reserved_expression(
        "array_slice",
        group="higher_order",
        name="array_slice",
        type=ArrayType(array.element, contains_null=array.contains_null),
        nullable=argument.nullable or offset.nullable or size.nullable,
        args=(argument, offset, size),
    )


def element_at(value: object, key: object) -> Expression:
    return _element_lookup("element_at", value, key)


def try_element_at(value: object, key: object) -> Expression:
    return _element_lookup("try_element_at", value, key)


def map_concat(*values: object, duplicates: str = "error") -> Expression:
    if duplicates != "error":
        raise TypeError('map_concat(...) currently supports duplicates="error" only')
    if len(values) < 2:
        raise TypeError("map_concat(...) requires at least two Map expressions")
    arguments = tuple(literal(value) for value in values)
    maps = tuple(_map_type(argument, "map_concat(...)") for argument in arguments)
    first = maps[0]
    if any(
        not _same_type(first.key, map_type.key) or not _same_type(first.value, map_type.value) for map_type in maps[1:]
    ):
        raise TypeError("map_concat(...) requires maps with matching key and value types")
    return _reserved_expression(
        "map_concat",
        group="higher_order",
        name="map_concat",
        type=MapType(
            first.key, first.value, value_contains_null=any(map_type.value_contains_null for map_type in maps)
        ),
        nullable=any(argument.nullable for argument in arguments),
        args=arguments,
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
    if result.nullable:
        raise TypeError("map_transform_keys(...) callback must return a non-null key expression")
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
    if not _same_type(left_map.key, right_map.key):
        raise TypeError("map_zip_with(...) requires matching map key types")
    key_type = left_map.key
    key = _lambda_arg(key_type, nullable=False, name="key")
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
        type=MapType(key_type, result_type, value_contains_null=result.nullable),
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
    map_type = _map_type(argument, "map_entries(...)")
    return _reserved_expression(
        "map_entries",
        group="higher_order",
        name="map_entries",
        type=ArrayType(_map_entry_type(map_type), contains_null=False),
        nullable=argument.nullable,
        args=(argument,),
    )


def map_from_entries(value: object) -> Expression:
    argument = literal(value)
    array = _array_type(argument, "map_from_entries(...)")
    entry = _map_entry_fields(array, "map_from_entries(...)")
    return _reserved_expression(
        "map_from_entries",
        group="higher_order",
        name="map_from_entries",
        type=MapType(entry["key"].type, entry["value"].type, value_contains_null=entry["value"].nullable),
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
    return OperationPlan.cache_operation(CachePlan(storage_level=_cache_storage_level(storage_level)))


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
        kind="transform_expression",
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


def _collection_type(expression: Expression, call: str) -> ArrayType | MapType:
    if not isinstance(expression.type, (ArrayType, MapType)):
        raise TypeError(f"{call} requires an Array or Map expression")
    return expression.type


def _map_type(expression: Expression, call: str) -> MapType:
    if not isinstance(expression.type, MapType):
        raise TypeError(f"{call} requires a Map expression")
    return expression.type


def _array_set_operation(function: str, left: object, right: object) -> Expression:
    left_argument = literal(left)
    right_argument = literal(right)
    left_array = _array_type(left_argument, f"{function}(...)")
    right_array = _array_type(right_argument, f"{function}(...)")
    element_type = _unify_types(f"{function}(...)", (left_array.element, right_array.element))
    return _reserved_expression(
        function,
        group="higher_order",
        name=function,
        type=ArrayType(element_type, contains_null=left_array.contains_null or right_array.contains_null),
        nullable=left_argument.nullable or right_argument.nullable,
        args=(left_argument, right_argument),
    )


def _array_mutation(function: str, value: object, item: object) -> Expression:
    argument = literal(value)
    array_type = _array_type(argument, f"{function.removeprefix('array_')}(...)")
    element = literal(item)
    element_type = _unify_types(
        f"{function.removeprefix('array_')}(...)", (array_type.element, _typed_type(function, element))
    )
    return _reserved_expression(
        function,
        group="higher_order",
        name=function,
        type=ArrayType(element_type, contains_null=array_type.contains_null or element.nullable),
        nullable=argument.nullable,
        args=(argument, element),
    )


def _element_lookup(function: str, value: object, key: object) -> Expression:
    argument = literal(value)
    lookup = literal(key)
    if isinstance(argument.type, ArrayType):
        if not isinstance(lookup.type, (IntegerType, LongType)):
            raise TypeError(f"{function}(...) Array index must be an integral Structure expression")
        if lookup.kind == "literal" and (lookup.data or {}).get("value") == 0:
            raise TypeError(f"{function}(...) Array index is one-based and cannot be zero")
        return _reserved_expression(
            function,
            group="higher_order",
            name=function,
            type=argument.type.element,
            nullable=True,
            args=(argument, lookup),
        )
    if isinstance(argument.type, MapType):
        _map_key_type(f"{function}(...)", argument.type, lookup)
        return _reserved_expression(
            function,
            group="higher_order",
            name=function,
            type=argument.type.value,
            nullable=True,
            args=(argument, lookup),
        )
    raise TypeError(f"{function}(...) requires an Array or Map expression")


def _map_key_type(call: str, map_type: MapType, key: Expression) -> None:
    if key.nullable or key.type is None:
        raise TypeError(f"{call} requires a non-null key with the map key type")
    try:
        _unify_types(call, (map_type.key, key.type))
    except TypeError as error:
        raise TypeError(
            f"{call} requires a non-null key with map key type {map_type.key.name}; received {key.type.name}"
        ) from error


def _sortable_type(call: str, expression: Expression) -> None:
    type = _typed_type(call, expression)
    if type.name not in {"date", "decimal", "double", "float", "integer", "long", "string", "timestamp"}:
        raise TypeError(f"{call} must return an orderable scalar expression; received {type.name}")


def _positive_interval(call: str, value: object) -> str:
    if not isinstance(value, str) or not fullmatch(
        r"\s*[1-9]\d*(?:\.\d+)?\s+(?:microseconds?|milliseconds?|seconds?|minutes?|hours?|days?|weeks?)\s*",
        value,
    ):
        raise TypeError(f"{call} requires a positive fixed Spark interval string, such as '10 minutes'")
    return value.strip()


def _cache_storage_level(value: object) -> tuple[bool, bool, bool, bool, int] | None:
    if value is True:
        return None
    names = ("useDisk", "useMemory", "useOffHeap", "deserialized", "replication")
    try:
        use_disk, use_memory, use_off_heap, deserialized, replication = (getattr(value, name) for name in names)
    except AttributeError as error:
        raise TypeError(
            "cache(...) requires True or a PySpark StorageLevel; omit cache= to leave a step uncached"
        ) from error
    if not all(isinstance(option, bool) for option in (use_disk, use_memory, use_off_heap, deserialized)) or (
        isinstance(replication, bool) or not isinstance(replication, int) or replication < 1
    ):
        raise TypeError("cache(...) requires a valid PySpark StorageLevel")
    return use_disk, use_memory, use_off_heap, deserialized, replication


@cached
def _map_entry_type(map_type: MapType) -> StructType:
    schema = type(
        "_MapEntry",
        (Schema,),
        {
            "key": FieldDeclaration(map_type.key, nullable=False),
            "value": FieldDeclaration(map_type.value, nullable=map_type.value_contains_null),
        },
    )
    return StructType(schema)


def _map_entry_fields(array: ArrayType, call: str):
    if array.contains_null:
        raise TypeError(f"{call} requires an Array of non-null key/value Struct entries")
    if not isinstance(array.element, StructType):
        raise TypeError(f"{call} requires an Array of key/value Struct entries")
    fields = array.element.schema._structure_fields
    if set(fields) != {"key", "value"}:
        raise TypeError(f"{call} requires Struct entries with exactly key and value fields")
    if fields["key"].nullable:
        raise TypeError(f"{call} requires non-null key fields")
    return fields


def _unified_argument_type(call: str, arguments: tuple[Expression, ...]) -> StructureType:
    types = tuple(argument.type for argument in arguments if argument.type is not None)
    if not types:
        raise TypeError(f"{call} requires at least one typed value; null-only arrays need an explicit typed value")
    return _unified_types(call, types)


def _typed_type(call: str, argument: Expression) -> StructureType:
    if argument.type is None:
        raise TypeError(f"{call} requires a typed value")
    return argument.type


def _unify_types(call: str, types: tuple[StructureType, ...]) -> StructureType:
    first = types[0]
    if all(_same_type(type, first) for type in types[1:]):
        return first
    if all(isinstance(type, (IntegerType, LongType, FloatType, DoubleType)) for type in types):
        if any(isinstance(type, DoubleType) for type in types):
            return DoubleType()
        if any(isinstance(type, FloatType) for type in types):
            return FloatType()
        if any(isinstance(type, LongType) for type in types):
            return LongType()
        return IntegerType()
    names = ", ".join(type.name for type in types)
    raise TypeError(f"{call} requires compatible types; received {names}")


def _same_type(left: StructureType, right: StructureType) -> bool:
    if left.name != right.name:
        return False
    if isinstance(left, ArrayType) and isinstance(right, ArrayType):
        return left.contains_null == right.contains_null and _same_type(left.element, right.element)
    if isinstance(left, MapType) and isinstance(right, MapType):
        return (
            left.value_contains_null == right.value_contains_null
            and _same_type(left.key, right.key)
            and _same_type(left.value, right.value)
        )
    if isinstance(left, StructType) and isinstance(right, StructType):
        return left.schema is right.schema
    if left.name == "decimal":
        return getattr(left, "precision") == getattr(right, "precision") and getattr(left, "scale") == getattr(
            right, "scale"
        )
    return True


def _unified_types(call: str, types: tuple[StructureType, ...]) -> StructureType:
    return _unify_types(call, types)


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
