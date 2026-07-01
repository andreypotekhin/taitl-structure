from structure.app.dsl.model.expr.Expression import Expression
from structure.app.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.app.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


class PySparkExpressionMapper:

    def map(self, expression: Expression, *, capabilities: BackendCapabilities) -> PySparkExpressionRecipe:
        group, name = self._requirement(expression)
        capabilities.require(CapabilityRequirement(group=group, name=name))
        return PySparkExpressionRecipe(
            kind=expression.kind,
            type=expression.type,
            nullable=expression.nullable,
            data=dict(expression.data or {}),
            args=tuple(self.map(argument, capabilities=capabilities) for argument in expression.args),
        )

    def _requirement(self, expression: Expression) -> tuple[str, str]:
        if expression.kind == "reserved_v2":
            data = expression.data or {}
            return str(data["capability_group"]), str(data["capability_name"])
        if expression.kind == "field":
            return "expression", "field_ref"
        if expression.kind == "literal":
            return "expression", "literal"
        if expression.kind in {"and", "or", "not", "is_null", "is_not_null"}:
            return "expression", "boolean_ops"
        if expression.kind in {"eq", "ne", "gt", "lt", "le", "ge"}:
            return "expression", "equality"
        if expression.kind == "null_safe_eq":
            return "expression", "null_safe_equality"
        if expression.kind in {"add", "sub", "mul", "when"}:
            return "expression", "standard_helper_call"
        if expression.kind == "call":
            function = (expression.data or {}).get("function")
            if function == "to_decimal":
                return "expression", "cast"
            return "expression", "standard_helper_call"
        return "expression", "standard_helper_call"
