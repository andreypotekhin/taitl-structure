from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class BuildPySparkUdfDiagnostics:
    """Report opaque Python UDFs captured in PySpark step bodies."""

    def __call__(self, plan: TransformPlan, *, enabled: bool) -> tuple[Diagnostic, ...]:
        if not enabled:
            return ()
        seen: set[str] = set()
        diagnostics: list[Diagnostic] = []
        for step in plan.steps:
            body = step.plugin_body
            if not isinstance(body, PySparkStepBody):
                continue
            for expression in self._expressions(body):
                for udf in self._udfs(expression):
                    data = getattr(udf, "data", None) or {}
                    qualname = str(data.get("qualname", data.get("function_name", "python_udf")))
                    if qualname in seen:
                        continue
                    seen.add(qualname)
                    owner = getattr(step.origin, "owner", None)
                    owner_name = getattr(owner, "__name__", plan.name)
                    owner_module = getattr(owner, "__module__", "")
                    diagnostics.append(
                        Diagnostic(
                            entry=diagnostic_registry.get("DSL-W0403"),
                            problem=f"{owner_name} uses Python UDF {qualname}; Spark cannot inspect or optimize the UDF body.",
                            use="Prefer Structure expression helpers when logic can stay compiler-visible, or set @transform(warn_on_udfs=False).",
                            context={"udf": qualname},
                            source=f"{owner_module}.{owner_name}".strip("."),
                        )
                    )
        return tuple(diagnostics)

    @staticmethod
    def _expressions(body: PySparkStepBody) -> tuple[object, ...]:
        return (
            *body.filters,
            *(assignment.expression for assignment in body.projection),
            *(
                operation.posexplode_struct.expression
                for operation in body.operations
                if operation.posexplode_struct is not None
            ),
            *(operation.filter for operation in body.operations if operation.filter is not None),
            *(assignment.expression for result in body.results for assignment in result.projection),
        )

    def _udfs(self, expression: object) -> tuple[object, ...]:
        found = [expression] if getattr(expression, "kind", None) == "python_udf" else []
        for argument in getattr(expression, "args", ()):
            found.extend(self._udfs(argument))
        return tuple(found)
