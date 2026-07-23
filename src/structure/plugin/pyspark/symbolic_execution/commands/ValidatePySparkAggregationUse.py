from structure import StructureCompileError
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class ValidatePySparkAggregationUse:
    """Validate aggregate-only PySpark symbolic state before lowering."""

    def __call__(self, body: PySparkStepBody, *, request: StepAuthoringRequest) -> None:
        if body.aggregate_keys is not None and len(request.results) > 1:
            self._error(
                request,
                f"uses group_by(...) with multiple returned schemas.",
                "Return one aggregate schema per grouped step method.",
            )
        if body.aggregate_having is not None and body.aggregate is None:
            self._error(
                request,
                "uses having(...) outside grouped aggregation.",
                "Call group_by(...), rollup(...), cube(...), or grouping_sets(...) before having(...).",
            )

    def _error(self, request: StepAuthoringRequest, problem: str, use: str) -> None:
        origin = request.origin
        class_name = getattr(origin, "class_name", "Transform")
        member = getattr(origin, "member_name", request.name)
        raise StructureCompileError(
            Diagnostic(
                entry=diagnostic_registry.get("DSL-E0402"),
                problem=f"{class_name}.{member} {problem}",
                use=use,
                context={},
                source=member,
            )
        )
