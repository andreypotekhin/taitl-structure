from typing import Any, cast

from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.api.v1.model.StepResultPlan import StepResultPlan
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan
from structure.plugin.pyspark.dsl.operations_api import cache_operation, reserved_operations
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkAggregates import ValidatePySparkAggregates
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkAggregationUse import (
    ValidatePySparkAggregationUse,
)
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkComparisons import ValidatePySparkComparisons
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkRelationReads import (
    ValidatePySparkRelationReads,
)
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
        body = PySparkStepBody(
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
        ValidatePySparkAggregationUse()(body, request=request)
        ValidatePySparkAggregates()(body, request=request)
        ValidatePySparkComparisons()(self._expressions(body), request=request)
        ValidatePySparkRelationReads()(body, request=request)
        return body

    def _expressions(self, body: PySparkStepBody) -> tuple[Expression, ...]:
        expressions: list[Expression] = [*body.filters, *(assignment.expression for assignment in body.projection)]
        for result in body.results:
            expressions.extend(assignment.expression for assignment in result.projection)
            if result.aggregate is None:
                continue
            expressions.extend(key.expression for key in result.aggregate.keys)
            expressions.extend(
                expression
                for assignment in result.aggregate.assignments
                for expression in (*assignment.arguments, assignment.filter, assignment.order_by)
                if expression is not None
            )
            if result.aggregate.having is not None:
                expressions.append(result.aggregate.having)
        return tuple(expressions)

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
