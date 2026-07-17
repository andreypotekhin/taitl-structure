from typing import cast

from structure.app.dsl.model.expr.Expression import Expression
from structure.app.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.app.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


class PySparkExpressionMapper:

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
            if function == "to_decimal":
                return "expression", "cast"
            return "expression", "standard_helper_call"
        if expression.kind == "python_udf":
            return "expression", "python_udf"
        if expression.kind == "time_window":
            return "streaming", "time_window"
        if expression.kind == "transform_expression" and (expression.data or {}).get("function") == "session_window":
            return "streaming", "session_window"
        return "expression", "standard_helper_call"
