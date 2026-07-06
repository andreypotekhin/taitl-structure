from __future__ import annotations

import json
from typing import Mapping

from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


class RenderPySparkExpression:

    def __call__(
        self,
        expression: PySparkExpressionRecipe,
        *,
        scope_aliases: Mapping[str, str] | None = None,
    ) -> str:
        aliases = scope_aliases or {}
        return self._render(expression, aliases)

    def _render(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        if expression.kind == "field":
            return self._field(expression, aliases)
        if expression.kind == "literal":
            return f"F.lit({expression.data['value']!r})"
        if expression.kind == "lambda_arg":
            return str(expression.data["name"])
        if expression.kind == "call":
            return self._call(expression, aliases)
        if expression.kind == "reserved_v2":
            return self._reserved(expression, aliases)
        if expression.kind == "is_not_null":
            return f"{self._render(expression.args[0], aliases)}.isNotNull()"
        if expression.kind == "is_null":
            return f"{self._render(expression.args[0], aliases)}.isNull()"
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
        if expression.kind == "when":
            condition, value, fallback = expression.args
            return (
                f"F.when({self._render(condition, aliases)}, {self._render(value, aliases)})"
                f".otherwise({self._render(fallback, aliases)})"
            )
        if expression.kind == "null_safe_eq":
            left, right = expression.args
            return f"{self._render(left, aliases)}.eqNullSafe({self._render(right, aliases)})"
        if expression.kind == "not":
            return f"~({self._render(expression.args[0], aliases)})"
        raise TypeError(f"Unsupported PySpark expression recipe: {expression.kind}")

    def _reserved(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        function = expression.data["function"]
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
            argument = self._lambda_name(body, "item")
            return f"F.exists({self._render(array, aliases)}, lambda {argument}: {self._render(body, aliases)})"
        if function == "array_forall":
            array, body = expression.args
            argument = self._lambda_name(body, "item")
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
            [array] = expression.args
            ascending = "False" if expression.data.get("descending") else "True"
            return f"F.sort_array({self._render(array, aliases)}, asc={ascending})"
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
            order_by, *partition_by = expression.args
            call = function.removeprefix("window_")
            return f"F.{call}().over({self._window(order_by, partition_by, expression, aliases, include_frame=False)})"
        if function in {"window_percent_rank", "window_cume_dist"}:
            order_by, *partition_by = expression.args
            call = function.removeprefix("window_")
            return f"F.{call}().over({self._window(order_by, partition_by, expression, aliases, include_frame=False)})"
        if function == "window_ntile":
            order_by, *partition_by = expression.args
            return (
                f"F.ntile({expression.data['buckets']}).over("
                f"{self._window(order_by, partition_by, expression, aliases, include_frame=False)})"
            )
        if function in {"window_lag", "window_lead"}:
            value, order_by, *partition_by = expression.args
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
            order_by = expression.args[value_count]
            partition_by = list(expression.args[value_count + 1:])
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
            "window_count_distinct",
        }:
            value_count = self._int_data(expression, "value_count", 1)
            values = expression.args[:value_count]
            order_by = expression.args[value_count]
            partition_by = list(expression.args[value_count + 1:])
            call = function.removeprefix("window_")
            function_name = "countDistinct" if call == "count_distinct" else call
            argument = self._render(values[0], aliases) if values else "F.lit(1)"
            return f"F.{function_name}({argument}).over({self._window(order_by, partition_by, expression, aliases)})"
        if function in {"window_rolling_sum", "window_rolling_avg", "window_rolling_min", "window_rolling_max"}:
            value, order_by, *partition_by = expression.args
            call = function.removeprefix("window_rolling_")
            return (
                f"F.{call}({self._render(value, aliases)})"
                f".over({self._window(order_by, partition_by, expression, aliases)})"
            )
        raise TypeError(f"Unsupported PySpark reserved expression: {function}")

    def _window(
        self,
        order_by: PySparkExpressionRecipe,
        partition_by: list[PySparkExpressionRecipe],
        expression: PySparkExpressionRecipe,
        aliases: Mapping[str, str],
        *,
        include_frame: bool = True,
    ) -> str:
        partitions = ", ".join(self._render(partition, aliases) for partition in partition_by)
        order = self._render(order_by, aliases)
        direction = "desc" if expression.data.get("descending") else "asc"
        window = f"Window.partitionBy({partitions}).orderBy({order}.{direction}())"
        if not include_frame:
            return window
        if "frame_kind" in expression.data:
            frame = "rowsBetween" if expression.data["frame_kind"] == "rows" else "rangeBetween"
            return f"{window}.{frame}({expression.data['frame_start']}, {expression.data['frame_end']})"
        if "preceding" in expression.data:
            return f"{window}.rowsBetween(-{expression.data['preceding']}, 0)"
        return window

    def _lambda_name(self, expression: PySparkExpressionRecipe, fallback: str) -> str:
        return str(expression.data.get("name", fallback)) if expression.kind == "lambda_arg" else fallback

    def _field(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        scope = str(expression.data["scope"])
        field = str(expression.data["field"])
        alias = aliases.get(scope, scope)
        return f"F.col({self._literal(f'{alias}.{field}')})"

    def _call(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        function = expression.data["function"]
        args = [self._render(argument, aliases) for argument in expression.args]
        if function == "lower":
            return f"F.lower({args[0]})"
        if function == "trim":
            return f"F.trim({args[0]})"
        if function == "upper":
            return f"F.upper({args[0]})"
        if function == "coalesce":
            return f"F.coalesce({', '.join(args)})"
        if function == "to_decimal":
            precision = expression.data["precision"]
            scale = expression.data["scale"]
            return f'{args[0]}.cast("decimal({precision},{scale})")'
        raise TypeError(f"Unsupported PySpark helper call: {function}")

    def _int_data(self, expression: PySparkExpressionRecipe, key: str, default: int) -> int:
        value = expression.data.get(key, default)
        return value if isinstance(value, int) else default

    def _binary(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str], operator: str) -> str:
        left, right = expression.args
        return f"({self._render(left, aliases)} {operator} {self._render(right, aliases)})"

    def _literal(self, value: str) -> str:
        return json.dumps(value)

    def _render_literal_value(self, expression: PySparkExpressionRecipe) -> str:
        return repr(expression.data["value"])


render_pyspark_expression = RenderPySparkExpression()
