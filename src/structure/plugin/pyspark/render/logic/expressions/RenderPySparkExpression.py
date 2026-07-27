from __future__ import annotations

import json
import re
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any, Mapping, cast

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe

_embed_exprs: ContextVar[bool] = ContextVar("structure_embed_exprs", default=False)


class RenderPySparkExpression:

    def __call__(
        self,
        expression: PySparkExpressionRecipe,
        *,
        scope_aliases: Mapping[str, str] | None = None,
    ) -> str:
        aliases = scope_aliases or {}
        return self._render(expression, aliases)

    @contextmanager
    def embed_exprs(self, enabled: bool):
        token: Token[bool] = _embed_exprs.set(enabled)
        try:
            yield
        finally:
            _embed_exprs.reset(token)

    def _render(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        if expression.kind == "field":
            return self._field(expression, aliases)
        if expression.kind == "get_field":
            return f"{self._render(expression.args[0], aliases)}.getField({expression.data['field']!r})"
        if expression.kind == "with_field":
            return (
                f"{self._render(expression.args[0], aliases)}.withField({expression.data['field']!r}, "
                f"{self._render(expression.args[1], aliases)})"
            )
        if expression.kind == "drop_fields":
            fields = ", ".join(repr(field) for field in cast(tuple[str, ...], expression.data["fields"]))
            return f"{self._render(expression.args[0], aliases)}.dropFields({fields})"
        if expression.kind == "struct":
            return self._struct(expression, aliases)
        if expression.kind == "literal":
            return f"F.lit({expression.data['value']!r})"
        if expression.kind == "lambda_arg":
            return str(expression.data["name"])
        if expression.kind == "call":
            return self._call(expression, aliases)
        if expression.kind == "python_udf":
            args = [self._render(argument, aliases) for argument in expression.args]
            return f"self.{expression.data['udf_name']}({', '.join(args)})"
        if expression.kind == "time_window":
            arguments = [self._render(expression.args[0], aliases), repr(expression.data["duration"])]
            if expression.data.get("slide") is not None:
                arguments.append(repr(expression.data["slide"]))
            if expression.data.get("start") is not None:
                arguments.append(repr(expression.data["start"]))
            return f"F.window({', '.join(arguments)})"
        if expression.kind == "special_expr":
            return self._special_expr(expression, aliases)
        if expression.kind == "transform_expression":
            return self._reserved(expression, aliases)
        if expression.kind == "is_not_null":
            return f"{self._render(expression.args[0], aliases)}.isNotNull()"
        if expression.kind == "is_null":
            return f"{self._render(expression.args[0], aliases)}.isNull()"
        if expression.kind == "is_nan":
            return f"F.isnan({self._render(expression.args[0], aliases)})"
        if expression.kind == "and":
            return self._binary(expression, aliases, "&")
        if expression.kind == "or":
            return self._binary(expression, aliases, "|")
        if expression.kind == "eq":
            return self._binary(expression, aliases, "==")
        if expression.kind == "ne":
            return self._binary(expression, aliases, "!=")
        if expression.kind == "gt":
            return self._binary(expression, aliases, ">")
        if expression.kind == "lt":
            return self._binary(expression, aliases, "<")
        if expression.kind == "le":
            return self._binary(expression, aliases, "<=")
        if expression.kind == "ge":
            return self._binary(expression, aliases, ">=")
        if expression.kind == "add":
            return self._binary(expression, aliases, "+")
        if expression.kind == "sub":
            return self._binary(expression, aliases, "-")
        if expression.kind == "mul":
            return self._binary(expression, aliases, "*")
        if expression.kind == "div":
            return self._binary(expression, aliases, "/")
        if expression.kind == "mod":
            return self._binary(expression, aliases, "%")
        if expression.kind == "neg":
            return f"(-{self._render(expression.args[0], aliases)})"
        if expression.kind in {"bitwise_and", "bitwise_or", "bitwise_xor"}:
            return self._bitwise_binary(expression, aliases)
        if expression.kind == "bitwise_not":
            return f"F.bitwise_not({self._render(expression.args[0], aliases)})"
        if expression.kind == "when":
            condition, value, fallback = expression.args
            return (
                f"F.when({self._render(condition, aliases)}, {self._render(value, aliases)})"
                f".otherwise({self._render(fallback, aliases)})"
            )
        if expression.kind == "null_safe_eq":
            left, right = expression.args
            return f"{self._render(left, aliases)}.eqNullSafe({self._render(right, aliases)})"
        if expression.kind == "isin":
            value, *items = expression.args
            rendered_items = ", ".join(self._render(item, aliases) for item in items)
            return f"{self._render(value, aliases)}.isin({rendered_items})"
        if expression.kind in {"contains", "startswith", "endswith", "like", "ilike", "rlike"}:
            return f"{self._render(expression.args[0], aliases)}.{expression.kind}({expression.data['pattern']!r})"
        if expression.kind == "item":
            collection, key = expression.args
            rendered_key = repr(key.data["value"]) if key.kind == "literal" else self._render(key, aliases)
            return f"{self._render(collection, aliases)}[{rendered_key}]"
        if expression.kind == "cast":
            return f"{self._render(expression.args[0], aliases)}.cast({expression.data['spark_type']!r})"
        if expression.kind == "try_cast":
            return f"{self._render(expression.args[0], aliases)}.try_cast({expression.data['spark_type']!r})"
        if expression.kind == "order":
            return f"{self._render(expression.args[0], aliases)}.{expression.data['direction']}()"
        if expression.kind == "event_time_between":
            left, right = expression.args
            lower = self._interval(str(expression.data["lower"]))
            upper = self._interval(str(expression.data["upper"]))
            rendered_left = self._render(left, aliases)
            rendered_right = self._render(right, aliases)
            return (
                f"(({rendered_right} >= ({rendered_left} - {lower})) "
                f"& ({rendered_right} <= ({rendered_left} + {upper})))"
            )
        if expression.kind == "not":
            return f"~({self._render(expression.args[0], aliases)})"
        raise TypeError(f"Unsupported PySpark expression recipe: {expression.kind}")

    def _special_expr(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        if not _embed_exprs.get():
            return self._render(cast(PySparkExpressionRecipe, expression.data["expanded"]), aliases)
        arguments = [self._render(argument, aliases) for argument in expression.args]
        arguments.extend(
            f"{name}={self._render(argument, aliases)}"
            for name, argument in cast(
                tuple[tuple[str, PySparkExpressionRecipe], ...], expression.data["keyword_arguments"]
            )
        )
        return f"self.{expression.data['name']}({', '.join(arguments)})"

    def _reserved(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        function = expression.data["function"]
        if function == "session_window":
            return f"F.session_window({self._render(expression.args[0], aliases)}, {expression.data['gap']!r})"
        if function == "array_transform":
            array, body = expression.args
            argument = self._lambda_name(body, "item")
            return f"F.transform({self._render(array, aliases)}, lambda {argument}: {self._render(body, aliases)})"
        if function == "array_filter":
            array, body = expression.args
            argument = self._lambda_name(body, "item")
            return f"F.filter({self._render(array, aliases)}, lambda {argument}: {self._render(body, aliases)})"
        if function == "array_exists":
            array, body = expression.args
            argument = str(expression.data.get("lambda_name", "item"))
            return f"F.exists({self._render(array, aliases)}, lambda {argument}: {self._render(body, aliases)})"
        if function == "array_forall":
            array, body = expression.args
            argument = str(expression.data.get("lambda_name", "item"))
            return f"F.forall({self._render(array, aliases)}, lambda {argument}: {self._render(body, aliases)})"
        if function == "array_zip_with":
            left, right, left_item, right_item, body = expression.args
            left_name = self._lambda_name(left_item, "left_item")
            right_name = self._lambda_name(right_item, "right_item")
            return (
                f"F.zip_with({self._render(left, aliases)}, {self._render(right, aliases)}, "
                f"lambda {left_name}, {right_name}: {self._render(body, aliases)})"
            )
        if function == "array_aggregate":
            array, initial, accumulator, item, merged, finished = expression.args
            acc_name = self._lambda_name(accumulator, "acc")
            item_name = self._lambda_name(item, "item")
            rendered = (
                f"F.aggregate({self._render(array, aliases)}, {self._render(initial, aliases)}, "
                f"lambda {acc_name}, {item_name}: {self._render(merged, aliases)}"
            )
            if finished != merged:
                rendered += f", lambda {acc_name}: {self._render(finished, aliases)}"
            return f"{rendered})"
        if function == "array_sort_by":
            array, left_key, right_key = expression.args
            comparator = self._array_sort_comparator(
                self._render(left_key, aliases),
                self._render(right_key, aliases),
                descending=bool(expression.data.get("descending")),
            )
            return f"F.array_sort({self._render(array, aliases)}, lambda left, right: {comparator})"
        if function == "array_sort":
            [array] = expression.args
            return f"F.array_sort({self._render(array, aliases)})"
        if function == "array_reverse":
            [array] = expression.args
            return f"F.reverse({self._render(array, aliases)})"
        if function == "array_flatten":
            [array] = expression.args
            return f"F.flatten({self._render(array, aliases)})"
        if function == "array_distinct":
            [array] = expression.args
            return f"F.array_distinct({self._render(array, aliases)})"
        if function == "array_position":
            array, item = expression.args
            rendered_item = self._render_literal_value(item) if item.kind == "literal" else self._render(item, aliases)
            return f"F.array_position({self._render(array, aliases)}, {rendered_item})"
        if function == "collection_size":
            [value] = expression.args
            return f"F.size({self._render(value, aliases)})"
        if function == "array_contains":
            array, item = expression.args
            rendered_item = self._render_literal_value(item) if item.kind == "literal" else self._render(item, aliases)
            return f"F.array_contains({self._render(array, aliases)}, {rendered_item})"
        if function == "map_contains_key":
            mapping, key = expression.args
            rendered_key = self._render_literal_value(key) if key.kind == "literal" else self._render(key, aliases)
            return f"F.map_contains_key({self._render(mapping, aliases)}, {rendered_key})"
        if function == "array":
            return f"F.array({', '.join(self._render(value, aliases) for value in expression.args)})"
        if function == "array_repeat":
            value, count = expression.args
            rendered_count = (
                self._render_literal_value(count) if count.kind == "literal" else self._render(count, aliases)
            )
            return f"F.array_repeat({self._render(value, aliases)}, {rendered_count})"
        if function == "array_sequence":
            return f"F.sequence({', '.join(self._render(value, aliases) for value in expression.args)})"
        if function in {"array_append", "array_prepend"}:
            array, item = expression.args
            return f"F.{function}({self._render(array, aliases)}, {self._render(item, aliases)})"
        if function == "array_insert":
            array, position, item = expression.args
            return (
                f"F.array_insert({self._render(array, aliases)}, {self._render_literal_value(position)}, "
                f"{self._render(item, aliases)})"
            )
        if function == "array_remove":
            array, item = expression.args
            return f"F.array_remove({self._render(array, aliases)}, {self._render_literal_value(item)})"
        if function == "array_compact":
            [array] = expression.args
            return f"F.array_compact({self._render(array, aliases)})"
        if function == "array_slice":
            value, start, length = expression.args
            rendered_start = (
                self._render_literal_value(start) if start.kind == "literal" else self._render(start, aliases)
            )
            rendered_length = (
                self._render_literal_value(length) if length.kind == "literal" else self._render(length, aliases)
            )
            return f"F.slice({self._render(value, aliases)}, {rendered_start}, {rendered_length})"
        if function in {"array_union", "array_except", "array_intersect"}:
            left, right = expression.args
            return f"F.{function}({self._render(left, aliases)}, {self._render(right, aliases)})"
        if function in {"element_at", "try_element_at"}:
            collection, key = expression.args
            rendered_key = self._render(key, aliases)
            return f"F.{function}({self._render(collection, aliases)}, {rendered_key})"
        if function == "map_concat":
            return f"F.map_concat({', '.join(self._render(value, aliases) for value in expression.args)})"
        if function == "map_transform_values":
            mapping, key, value, body = expression.args
            key_name = self._lambda_name(key, "key")
            value_name = self._lambda_name(value, "value")
            return (
                f"F.transform_values({self._render(mapping, aliases)}, "
                f"lambda {key_name}, {value_name}: {self._render(body, aliases)})"
            )
        if function == "map_filter":
            mapping, key, value, body = expression.args
            key_name = self._lambda_name(key, "key")
            value_name = self._lambda_name(value, "value")
            return (
                f"F.map_filter({self._render(mapping, aliases)}, "
                f"lambda {key_name}, {value_name}: {self._render(body, aliases)})"
            )
        if function == "map_transform_keys":
            mapping, key, value, body = expression.args
            key_name = self._lambda_name(key, "key")
            value_name = self._lambda_name(value, "value")
            return (
                f"F.transform_keys({self._render(mapping, aliases)}, "
                f"lambda {key_name}, {value_name}: {self._render(body, aliases)})"
            )
        if function == "map_zip_with":
            mapping, other, key, left_value, right_value, body = expression.args
            key_name = self._lambda_name(key, "key")
            left_name = self._lambda_name(left_value, "left_value")
            right_name = self._lambda_name(right_value, "right_value")
            return (
                f"F.map_zip_with({self._render(mapping, aliases)}, {self._render(other, aliases)}, "
                f"lambda {key_name}, {left_name}, {right_name}: {self._render(body, aliases)})"
            )
        if function == "map_keys":
            [mapping] = expression.args
            return f"F.map_keys({self._render(mapping, aliases)})"
        if function == "map_values":
            [mapping] = expression.args
            return f"F.map_values({self._render(mapping, aliases)})"
        if function == "map_entries":
            [mapping] = expression.args
            return f"F.map_entries({self._render(mapping, aliases)})"
        if function == "map_from_entries":
            [array] = expression.args
            return f"F.map_from_entries({self._render(array, aliases)})"
        if function in {"window_row_number", "window_rank", "window_dense_rank"}:
            order_by, partition_by = self._window_arguments(expression, 0)
            call = function.removeprefix("window_")
            return (
                f"F.{call}().over({self._window(order_by, partition_by, expression, aliases, include_frame=False)})"
                ".cast(T.LongType())"
            )
        if function in {"window_percent_rank", "window_cume_dist"}:
            order_by, partition_by = self._window_arguments(expression, 0)
            call = function.removeprefix("window_")
            return f"F.{call}().over({self._window(order_by, partition_by, expression, aliases, include_frame=False)})"
        if function == "window_ntile":
            order_by, partition_by = self._window_arguments(expression, 0)
            return (
                f"F.ntile({expression.data['buckets']}).over("
                f"{self._window(order_by, partition_by, expression, aliases, include_frame=False)})"
            )
        if function in {"window_lag", "window_lead"}:
            [value] = expression.args[:1]
            order_by, partition_by = self._window_arguments(expression, 1)
            call = function.removeprefix("window_")
            offset = expression.data["offset"]
            default = f", {expression.data['default']!r}" if expression.data.get("has_default") else ""
            return (
                f"F.{call}({self._render(value, aliases)}, {offset}{default})"
                f".over({self._window(order_by, partition_by, expression, aliases)})"
            )
        if function in {"window_first_value", "window_last_value", "window_nth_value"}:
            value_count = self._int_data(expression, "value_count", 1)
            values = expression.args[:value_count]
            order_by, partition_by = self._window_arguments(expression, value_count)
            call = function.removeprefix("window_")
            if call == "nth_value":
                arguments = f"{self._render(values[0], aliases)}, {expression.data['n']}"
            else:
                arguments = self._render(values[0], aliases)
            if expression.data.get("ignore_nulls"):
                arguments += ", True"
            return f"F.{call}({arguments}).over({self._window(order_by, partition_by, expression, aliases)})"
        if function in {
            "window_sum",
            "window_avg",
            "window_min",
            "window_max",
            "window_count",
            "window_bool_and",
            "window_bool_or",
            "window_stddev",
            "window_variance",
            "window_collect_list",
            "window_collect_set",
        }:
            value_count = self._int_data(expression, "value_count", 1)
            values = expression.args[:value_count]
            order_by, partition_by = self._window_arguments(expression, value_count)
            call = function.removeprefix("window_")
            argument = self._render(values[0], aliases) if values else "F.lit(1)"
            return f"F.{call}({argument}).over({self._window(order_by, partition_by, expression, aliases)})"
        if function in {"window_rolling_sum", "window_rolling_avg", "window_rolling_min", "window_rolling_max"}:
            [value] = expression.args[:1]
            order_by, partition_by = self._window_arguments(expression, 1)
            call = function.removeprefix("window_rolling_")
            return (
                f"F.{call}({self._render(value, aliases)})"
                f".over({self._window(order_by, partition_by, expression, aliases)})"
            )
        raise TypeError(f"Unsupported PySpark reserved expression: {function}")

    def _window(
        self,
        order_by: list[PySparkExpressionRecipe],
        partition_by: list[PySparkExpressionRecipe],
        expression: PySparkExpressionRecipe,
        aliases: Mapping[str, str],
        *,
        include_frame: bool = True,
    ) -> str:
        partitions = ", ".join(self._render(partition, aliases) for partition in partition_by)
        ordering = ", ".join(self._window_order(order, expression, aliases) for order in order_by)
        window = f"Window.partitionBy({partitions}).orderBy({ordering})"
        if not include_frame:
            return window
        if "frame_kind" in expression.data:
            frame = "rowsBetween" if expression.data["frame_kind"] == "rows" else "rangeBetween"
            return f"{window}.{frame}({expression.data['frame_start']}, {expression.data['frame_end']})"
        if "preceding" in expression.data:
            return f"{window}.rowsBetween(-{expression.data['preceding']}, 0)"
        return window

    def _array_sort_comparator(self, left: str, right: str, *, descending: bool) -> str:
        null_order = 1 if descending else -1
        value_order = 1 if descending else -1
        return (
            f"F.when(({left}.isNull() & {right}.isNotNull()), F.lit({null_order}))"
            f".when(({left}.isNotNull() & {right}.isNull()), F.lit({-null_order}))"
            f".when({left} < {right}, F.lit({value_order}))"
            f".when({left} > {right}, F.lit({-value_order}))"
            ".otherwise(F.lit(0))"
        )

    def _window_arguments(
        self, expression: PySparkExpressionRecipe, value_count: int
    ) -> tuple[list[PySparkExpressionRecipe], list[PySparkExpressionRecipe]]:
        order_count = self._int_data(expression, "order_count", 1)
        orders = list(expression.args[value_count : value_count + order_count])
        return orders, list(expression.args[value_count + order_count :])

    def _window_order(
        self, order: PySparkExpressionRecipe, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]
    ) -> str:
        rendered = self._render(order, aliases)
        return (
            rendered
            if order.kind == "order"
            else f"{rendered}.{'desc' if expression.data.get('descending') else 'asc'}()"
        )

    def _lambda_name(self, expression: PySparkExpressionRecipe, fallback: str) -> str:
        return str(expression.data.get("name", fallback)) if expression.kind == "lambda_arg" else fallback

    def _field(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        if "scope" not in expression.data:
            path = expression.data.get("name_path")
            if isinstance(path, tuple) and path:
                root, *fields = (str(segment) for segment in path)
                rendered = root
                for field in fields:
                    rendered += f".getField({field!r})"
                return rendered
        scope = str(expression.data["scope"])
        field = ".".join(self._field_path(expression))
        alias = aliases.get(scope, scope)
        if alias == "":
            return f"F.col({self._literal(field)})"
        return f"F.col({self._literal(f'{alias}.{field}')})"

    def _field_path(self, expression: PySparkExpressionRecipe) -> tuple[str, ...]:
        path = expression.data.get("path")
        if not isinstance(path, tuple):
            return (str(expression.data["field"]),)
        return tuple(self._field_segment(str(segment)) for segment in path)

    def _field_segment(self, value: str) -> str:
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", value):
            return value
        return f"`{value.replace('`', '``')}`"

    def _call(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        function = expression.data["function"]
        args = [self._render(argument, aliases) for argument in expression.args]
        if function in {"lower", "ltrim", "rtrim", "trim", "upper"}:
            return f"F.{function}({args[0]})"
        if function == "coalesce":
            return f"F.coalesce({', '.join(args)})"
        if function in {"nvl", "ifnull"}:
            return f"F.{function}({args[0]}, {args[1]})"
        if function == "nvl2":
            return f"F.nvl2({args[0]}, {args[1]}, {args[2]})"
        if function == "zeroifnull":
            return f"F.zeroifnull({args[0]})"
        if function == "nullif":
            return f"F.nullif({args[0]}, {args[1]})"
        if function == "nanvl":
            return f"F.nanvl({args[0]}, {args[1]})"
        if function == "to_decimal":
            precision = expression.data["precision"]
            scale = expression.data["scale"]
            return f'{args[0]}.cast("decimal({precision},{scale})")'
        if function == "substring":
            return f"F.substring({args[0]}, {expression.data['start']}, {expression.data['length']})"
        if function == "split":
            return f"F.split({args[0]}, {expression.data['pattern']!r}, {expression.data['limit']})"
        if function == "regexp_replace":
            return f"F.regexp_replace({args[0]}, {expression.data['pattern']!r}, {expression.data['replacement']!r})"
        if function == "regexp_extract":
            return f"F.regexp_extract({args[0]}, {expression.data['pattern']!r}, {expression.data['group']})"
        if function == "length":
            return f"F.length({args[0]})"
        if function in {"initcap", "reverse"}:
            return f"F.{function}({args[0]})"
        if function == "translate":
            return f"F.translate({args[0]}, {expression.data['matching']!r}, {expression.data['replacement']!r})"
        if function == "instr":
            return f"F.instr({args[0]}, {expression.data['substring']!r})"
        if function == "levenshtein":
            return f"F.levenshtein({args[0]}, {args[1]})"
        if function == "concat_ws":
            return f"F.concat_ws({expression.data['separator']!r}, {', '.join(args)})"
        if function in {"hash", "xxhash64"}:
            return f"F.{function}({', '.join(args)})"
        if function in {"md5", "sha1"}:
            return f"F.{function}({args[0]})"
        if function == "sha2":
            return f"F.sha2({args[0]}, {expression.data['bits']})"
        if function == "date_add":
            days = expression.data.get("days", args[1] if len(args) == 2 else None)
            return f"F.date_add({args[0]}, {days})"
        if function == "date_sub":
            return f"F.date_sub({args[0]}, {expression.data['days']})"
        if function == "datediff":
            return f"F.datediff({args[0]}, {args[1]})"
        if function == "date_trunc":
            return f"F.date_trunc({expression.data['unit']!r}, {args[0]})"
        if function == "trunc":
            return f"F.trunc({args[0]}, {expression.data['unit']!r})"
        if function in {"year", "month", "dayofmonth", "hour", "minute", "second"}:
            return f"F.{function}({args[0]})"
        if function in {"to_date", "to_timestamp"}:
            return (
                f"F.{function}({args[0]}, {expression.data['format']!r})"
                if "format" in expression.data
                else f"F.{function}({args[0]})"
            )
        if function == "abs":
            return f"F.abs({args[0]})"
        if function == "round":
            return f"F.round({args[0]}, {expression.data['scale']})"
        if function == "bround":
            return f"F.bround({args[0]}, {expression.data['scale']})"
        if function in {"ceil", "floor"}:
            return f"F.{function}({args[0]})"
        if function in {"sqrt", "exp", "signum"}:
            return f"F.{function}({args[0]})"
        if function == "pow":
            return f"F.pow({args[0]}, {args[1]})"
        if function == "log":
            return (
                f"F.log({expression.data['base']!r}, {args[0]})" if "base" in expression.data else f"F.log({args[0]})"
            )
        raise TypeError(f"Unsupported PySpark helper call: {function}")

    def _struct(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        fields = cast(tuple[Any, ...], expression.data["fields"])
        columns = (
            f"{self._render(argument, aliases)}.alias({self._literal(field.column)})"
            for field, argument in zip(fields, expression.args, strict=True)
        )
        return f"F.struct({', '.join(columns)})"

    def _int_data(self, expression: PySparkExpressionRecipe, key: str, default: int) -> int:
        value = expression.data.get(key, default)
        return value if isinstance(value, int) else default

    def _binary(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str], operator: str) -> str:
        left, right = expression.args
        return f"({self._render(left, aliases)} {operator} {self._render(right, aliases)})"

    def _bitwise_binary(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        left, right = expression.args
        method = {
            "bitwise_and": "bitwiseAND",
            "bitwise_or": "bitwiseOR",
            "bitwise_xor": "bitwiseXOR",
        }[expression.kind]
        return f"{self._render(left, aliases)}.{method}({self._render(right, aliases)})"

    def _literal(self, value: str) -> str:
        return json.dumps(value)

    def _interval(self, value: str) -> str:
        return f"F.expr({self._literal(f'INTERVAL {value}')})"

    def _render_literal_value(self, expression: PySparkExpressionRecipe) -> str:
        return repr(expression.data["value"])


render_pyspark_expression = RenderPySparkExpression()
