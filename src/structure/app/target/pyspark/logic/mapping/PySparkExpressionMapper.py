from structure.app.dsl.model.expr.Expression import Expression
from structure.app.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.app.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


class PySparkExpressionMapper:

    def map(self, expression: Expression, *, capabilities: BackendCapabilities) -> PySparkExpressionRecipe:
        capabilities.require(CapabilityRequirement(group="expression", name=self._requirement(expression)))
        return PySparkExpressionRecipe(
            kind=expression.kind,
            type=expression.type,
            nullable=expression.nullable,
            data=dict(expression.data or {}),
            args=tuple(self.map(argument, capabilities=capabilities) for argument in expression.args),
        )

    def _requirement(self, expression: Expression) -> str:
        if expression.kind == "field":
            return "field_ref"
        if expression.kind == "literal":
            return "literal"
        if expression.kind in {"and", "or", "not", "is_null", "is_not_null"}:
            return "boolean_ops"
        if expression.kind in {"eq", "ne", "gt"}:
            return "equality"
        if expression.kind == "null_safe_eq":
            return "null_safe_equality"
        if expression.kind == "sub":
            return "standard_helper_call"
        if expression.kind == "call":
            function = (expression.data or {}).get("function")
            if function == "to_decimal":
                return "cast"
            return "standard_helper_call"
        return "standard_helper_call"
