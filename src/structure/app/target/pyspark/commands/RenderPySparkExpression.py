from __future__ import annotations

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
        if expression.kind == "call":
            return self._call(expression, aliases)
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
        if expression.kind == "sub":
            return self._binary(expression, aliases, "-")
        if expression.kind == "null_safe_eq":
            left, right = expression.args
            return f"{self._render(left, aliases)}.eqNullSafe({self._render(right, aliases)})"
        if expression.kind == "not":
            return f"~({self._render(expression.args[0], aliases)})"
        raise TypeError(f"Unsupported PySpark expression recipe: {expression.kind}")

    def _field(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        scope = str(expression.data["scope"])
        field = str(expression.data["field"])
        alias = aliases.get(scope, scope)
        return f'F.col("{alias}.{field}")'

    def _call(self, expression: PySparkExpressionRecipe, aliases: Mapping[str, str]) -> str:
        function = expression.data["function"]
        args = [self._render(argument, aliases) for argument in expression.args]
        if function == "lower":
            return f"F.lower({args[0]})"
        if function == "trim":
            return f"F.trim({args[0]})"
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


render_pyspark_expression = RenderPySparkExpression()
