from structure import StructureCompileError
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.pyspark.dsl.Expression import Expression


class ValidatePySparkComparisons:
    """Reject PySpark expressions that record an incompatible comparison."""

    def __call__(
        self, expressions: tuple[Expression, ...] | list[Expression], *, request: StepAuthoringRequest
    ) -> None:
        for expression in expressions:
            if problem := self._problem(expression):
                origin = request.origin
                class_name = getattr(origin, "class_name", "Transform")
                member = getattr(origin, "member_name", request.name)
                raise StructureCompileError(
                    Diagnostic(
                        entry=diagnostic_registry.get("DSL-E0402"),
                        problem=problem,
                        use="Compare compatible Structure values, or use an explicit cast before comparing them.",
                        context={},
                        source=f"{getattr(origin, 'module', '')}.{class_name}.{member}".lstrip("."),
                    )
                )

    def _problem(self, expression: Expression) -> str | None:
        problem = (expression.data or {}).get("comparison_problem")
        if isinstance(problem, str):
            return problem
        return next((value for argument in expression.args if (value := self._problem(argument))), None)
