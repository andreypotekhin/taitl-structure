from typing import Any, cast

from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.api.v1.model.StepResultPlan import StepResultPlan
from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan
from structure.plugin.pyspark.dsl.operations_api import cache_operation, reserved_operations
from structure.plugin.pyspark.symbolic_execution.model.PySparkResultBody import PySparkResultBody
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody
from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import PySparkSymbolicContext


class CapturePySparkStep:
    """Freeze the private PySpark symbolic context into an opaque step body."""

    def __call__(
        self,
        value: object,
        *,
        context: PySparkSymbolicContext,
        request: StepAuthoringRequest,
    ) -> PySparkStepBody:
        context.operations.extend(self._reserved_operations(request))
        return PySparkStepBody(
            value=value,
            filters=tuple(context.filters),
            joins=tuple(context.joins),
            operations=tuple(context.operations),
            aggregate_keys=context.aggregate_keys,
            aggregate_levels=context.aggregate_levels,
            aggregate_grouping=context.aggregate_grouping,
            aggregate_having=context.aggregate_having,
            projection=context.projection,
            aggregate=context.aggregate,
            results=tuple(
                PySparkResultBody(
                    projection=tuple(cast(Any, result).projection),
                    aggregate=cast(Any, result).aggregate,
                )
                for result in cast(tuple[StepResultPlan, ...], context.results)
            ),
        )

    def _reserved_operations(self, request: StepAuthoringRequest) -> tuple[OperationPlan, ...]:
        origin = request.origin
        owner = getattr(origin, "owner", None)
        name = getattr(origin, "member_name", None)
        member = getattr(owner, name, None) if owner is not None and isinstance(name, str) else None
        if member is None:
            return ()
        metadata = getattr(member, "_structure_output_method", None)
        declared = () if not isinstance(metadata, dict) else tuple(metadata.get("reserved_operations", ()))
        declared = tuple(cache_operation(value) for kind, value in declared if kind == "cache")
        return (*reserved_operations(member), *declared)
