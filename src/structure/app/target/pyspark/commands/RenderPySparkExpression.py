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
        if function in {"window_row_number", "window_rank", "window_dense_rank"}:
            order_by, *partition_by = expression.args
            call = function.removeprefix("window_")
            return f"F.{call}().over({self._window(order_by, partition_by, expression, aliases)})"
        if function in {"window_lag", "window_lead"}:
            value, order_by, *partition_by = expression.args
            call = function.removeprefix("window_")
            offset = expression.data["offset"]
            default = f", {expression.data['default']!r}" if expression.data.get("has_default") else ""
            return (
                f"F.{call}({self._render(value, aliases)}, {offset}{default})"
                f".over({self._window(order_by, partition_by, expression, aliases)})"
            )
        raise TypeError(f"Unsupported PySpark reserved expression: {function}")

    def _window(
        self,
        order_by: PySparkExpressionRecipe,
        partition_by: list[PySparkExpressionRecipe],
        expression: PySparkExpressionRecipe,
        aliases: Mapping[str, str],
    ) -> str:
        partitions = ", ".join(self._render(partition, aliases) for partition in partition_by)
        order = self._render(order_by, aliases)
        direction = "desc" if expression.data.get("descending") else "asc"
        return f"Window.partitionBy({partitions}).orderBy({order}.{direction}())"

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

    def _binary(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str], operator: str) -> str:
        left, right = expression.args
        return f"({self._render(left, aliases)} {operator} {self._render(right, aliases)})"

    def _literal(self, value: str) -> str:
        return json.dumps(value)


render_pyspark_expression = RenderPySparkExpression()
