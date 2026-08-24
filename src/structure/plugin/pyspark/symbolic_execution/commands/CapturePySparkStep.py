from dataclasses import replace
from typing import Any, cast

from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.api.v1.model.StepResultPlan import StepResultPlan
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.operations.CacheOperations import cache_operation, reserved_operations
from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkAggregates import ValidatePySparkAggregates
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkAggregationUse import (
    ValidatePySparkAggregationUse,
)
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkComparisons import ValidatePySparkComparisons
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkRelationReads import (
    ValidatePySparkRelationReads,
)
from structure.plugin.pyspark.symbolic_execution.logic.results.BuildPySparkResultBodies import BuildPySparkResultBodies
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
        results = BuildPySparkResultBodies(request)(value, context=context)
        first = results[0]
        if first.aggregate is not None:
            context.record_aggregate(first.aggregate)
        operations = tuple(
            replace(operation, source_span=request.primary_span)
            if request.primary_span is not None
            else operation
            for operation in context.operations
        )
        body = PySparkStepBody(
            value=value,
            filters=tuple(context.filters),
            joins=tuple(context.joins),
            operations=operations,
            aggregate_keys=context.aggregate_keys,
            aggregate_levels=context.aggregate_levels,
            aggregate_grouping=context.aggregate_grouping,
            aggregate_having=context.aggregate_having,
            projection=first.projection,
            aggregate=first.aggregate,
            results=results,
        )
        ValidatePySparkAggregationUse()(body, request=request)
        ValidatePySparkAggregates()(body, request=request)
        ValidatePySparkComparisons()(self._expressions(body), request=request)
        ValidatePySparkRelationReads()(body, request=request)
        return body

    def _expressions(self, body: PySparkStepBody) -> tuple[Expression, ...]:
        expressions: list[Expression] = [*body.filters, *(assignment.expression for assignment in body.projection)]
        expressions.extend(
            operation.posexplode_struct.expression
            for operation in body.operations
            if operation.posexplode_struct is not None
        )
        expressions.extend(
            operation.scalar_generator.expression
            for operation in body.operations
            if operation.scalar_generator is not None
        )
        expressions.extend(
            operation.map_generator.expression for operation in body.operations if operation.map_generator is not None
        )
        expressions.extend(
            expression
            for operation in body.operations
            if operation.relation_order is not None
            for expression in operation.relation_order.order_by
        )
        expressions.extend(
            expression
            for operation in body.operations
            if operation.relation_hierarchy_closure is not None
            for expression in (operation.relation_hierarchy_closure.id, operation.relation_hierarchy_closure.parent)
        )
        expressions.extend(
            expression
            for operation in body.operations
            if operation.relation_hierarchy_fallback is not None
            for expression in (
                operation.relation_hierarchy_fallback.source_id,
                operation.relation_hierarchy_fallback.path,
                operation.relation_hierarchy_fallback.parent_id,
                operation.relation_hierarchy_fallback.parent,
            )
        )
        expressions.extend(
            expression
            for operation in body.operations
            if operation.relation_assertion is not None
            for expression in (
                *operation.relation_assertion.keys,
                operation.relation_assertion.predicate,
                operation.relation_assertion.value,
                operation.relation_assertion.reference_key,
                operation.relation_assertion.parent,
                operation.relation_assertion.order_by,
            )
            if expression is not None
        )
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
