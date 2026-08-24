from typing import cast

from structure.plugin.api.v1.model import BackendCapabilities, CapabilityRequirement
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.dsl.Expression import Expression


class MapPySparkExpression:

    def map(self, expression: Expression, *, capabilities: BackendCapabilities) -> PySparkExpressionRecipe:
        group, name = self._requirement(expression)
        capabilities.require(CapabilityRequirement(group=group, name=name))
        data = dict(expression.data or {})
        if expression.kind == "special_expr":
            data["body"] = self.map(cast(Expression, data["body"]), capabilities=capabilities)
            data["expanded"] = self.map(cast(Expression, data["expanded"]), capabilities=capabilities)
            data["keyword_arguments"] = tuple(
                (name, self.map(argument, capabilities=capabilities))
                for name, argument in cast(tuple[tuple[str, Expression], ...], data["keyword_arguments"])
            )
        return PySparkExpressionRecipe(
            kind=expression.kind,
            type=expression.type,
            nullable=expression.nullable,
            data=data,
            args=tuple(self.map(argument, capabilities=capabilities) for argument in expression.args),
        )

    def _requirement(self, expression: Expression) -> tuple[str, str]:
        if expression.kind == "transform_expression":
            data = expression.data or {}
            return str(data["capability_group"]), str(data["capability_name"])
        if expression.kind == "lambda_arg":
            return "expression", "standard_helper_call"
        if expression.kind == "field":
            return "expression", "field_ref"
        if expression.kind == "struct":
            return "expression", "standard_helper_call"
        if expression.kind == "literal":
            return "expression", "literal"
        if expression.kind in {"and", "or", "not", "is_null", "is_not_null", "is_nan"}:
            return "expression", "boolean_ops"
        if expression.kind in {"eq", "ne", "gt", "lt", "le", "ge"}:
            return "expression", "equality"
        if expression.kind == "null_safe_eq":
            return "expression", "null_safe_equality"
        if expression.kind == "cast":
            return "expression", "cast"
        if expression.kind == "try_cast":
            return "expression", "try_cast"
        if expression.kind in {"add", "sub", "mul", "div", "mod", "neg", "when"}:
            return "expression", "standard_helper_call"
        if expression.kind in {"bitwise_and", "bitwise_or", "bitwise_xor", "bitwise_not"}:
            return "expression", "bitwise"
        if expression.kind == "call":
            function = (expression.data or {}).get("function")
            if function == "is_valid_variant":
                return "expression", "is_valid_variant"
            if function == "rand":
                return "expression", "rand"
            if function in {
                "variant_array_append",
                "try_variant_array_append",
                "variant_insert",
                "try_variant_insert",
                "variant_set",
                "try_variant_set",
                "variant_delete",
            }:
                return "expression", str(function)
            if function in {
                "parse_json",
                "schema_of_variant",
                "try_parse_json",
                "variant_get",
                "try_variant_get",
                "to_variant_object",
                "is_variant_null",
            }:
                return "expression", "variant"
            if function in {"geo_from_wkt", "geo_as_wkt", "geo_intersects", "geo_contains", "geo_within"}:
                return "geo", "geometry"
            if function == "to_decimal":
                return "expression", "cast"
            return "expression", "standard_helper_call"
        if expression.kind == "python_udf":
            return "expression", "python_udf"
        if expression.kind == "time_window":
            return "streaming", "time_window"
        if expression.kind == "window_time":
            return "streaming", "window_time"
        if expression.kind == "transform_expression" and (expression.data or {}).get("function") == "session_window":
            return "streaming", "session_window"
        return "expression", "standard_helper_call"
