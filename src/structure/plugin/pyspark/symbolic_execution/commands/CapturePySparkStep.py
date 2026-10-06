from dataclasses import replace
from typing import Any, cast

from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.api.v1.model.StepResultPlan import StepResultPlan
from structure.plugin.api.v1.model.StepSinkCapture import StepSinkCapture
from structure.plugin.pyspark.delta.model import DeltaMutationResult
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.model.Projection import Projection
from structure.plugin.pyspark.dsl.operations.CacheOperations import cache_operation, reserved_operations
from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkAggregates import ValidatePySparkAggregates
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkAggregationUse import (
    ValidatePySparkAggregationUse,
)
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkComparisons import ValidatePySparkComparisons
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkRelationReads import (
    ValidatePySparkRelationReads,
)
from structure.plugin.pyspark.symbolic_execution.logic.results.BuildPySparkResultBodies import BuildPySparkResultBodies
from structure.plugin.pyspark.symbolic_execution.logic.results.ValidatePySparkResultReturn import (
    ValidatePySparkResultReturn,
)
from structure.plugin.pyspark.symbolic_execution.model.PySparkResultBody import PySparkResultBody
from structure.plugin.pyspark.symbolic_execution.model.PySparkSinkEffect import PySparkSinkEffect
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
        results: tuple[PySparkResultBody, ...]
        if request.sink_effect:
            if context.operations or context.filters or context.joins or len(context.foreach) != 1:
                raise TypeError("A sink-effect step must contain exactly one foreach(row, sink) call.")
            if not isinstance(value, PySparkSinkEffect) or value.capture is not context.foreach[0]:
                raise TypeError("A sink-effect step must return foreach(row, sink) directly.")
            results = (PySparkResultBody(),)
        elif request.effect:
            if context.operations or context.filters or context.joins or not context.delta_mutations:
                raise TypeError(f"Delta effect step {request.name} must contain Delta mutations only")
            if value is None:
                if any(mutation.target != request.results[0].lane for mutation in context.delta_mutations):
                    raise TypeError(
                        f"Delta effect step {request.name} must target its declared Delta table result"
                    )
            elif isinstance(value, DeltaMutationResult):
                if len(context.delta_mutations) != 1 or value.mutation is not context.delta_mutations[0]:
                    raise TypeError("A typed Delta effect step must return its sole mutation result directly")
                result = request.results[0]
                if value.mutation.kind == "restore":
                    if result.binding not in {"delta", "delta_table"}:
                        raise TypeError("A Delta restore step must resolve to delta_table(...) or delta_output(...)")
                    expected_binding = "delta_table" if result.binding == "delta_table" else "delta_input"
                    target = next(
                        (
                            item
                            for item in request.inputs
                            if item.source == value.mutation.target and item.binding == expected_binding
                        ),
                        None,
                    )
                    if target is None:
                        raise TypeError("A Delta restore result must bind to its declared caller-supplied table")
                    if result.binding == "delta_table" and target.schema is not result.schema:
                        raise TypeError("A same-schema Delta restore must return the delta_table(...) Schema")
                    if result.binding == "delta" and target.schema is result.schema:
                        raise TypeError("A shape-changing Delta restore must return a distinct delta_output(...) Schema")
                    mutation = replace(
                        value.mutation,
                        output=result.lane,
                        output_schema=cast(type, result.schema),
                    )
                    context.delta_mutations[0] = mutation
                    value = DeltaMutationResult(mutation)
                elif value.mutation.schema_evolution:
                    if result.binding not in {"delta", "delta_table"}:
                        raise TypeError(
                            "A schema-evolving Delta step must resolve to delta_table(...) or delta_output(...)"
                        )
                    expected_binding = "delta_table" if result.binding == "delta_table" else "delta_input"
                    target = next(
                        (
                            item
                            for item in request.inputs
                            if item.source == value.mutation.target and item.binding == expected_binding
                        ),
                        None,
                    )
                    if target is None:
                        raise TypeError(
                            "Schema evolution must target its declared delta_table(...) or delta_input(...) relation"
                        )
                    if result.binding == "delta_table":
                        if target.schema is not result.schema:
                            raise TypeError("A delta_table(...) effect must retain its declared relation Schema")
                        mutation = value.mutation
                    else:
                        if value.mutation.output_schema is not result.schema:
                            actual = getattr(value.mutation.output_schema, "__name__", "unknown Schema")
                            expected = getattr(result.schema, "__name__", str(result.schema))
                            raise TypeError(
                                f"with_schema_evolution(to=...) selected {actual}, but the declared "
                                f"delta_output(...) Schema is {expected}"
                            )
                        mutation = replace(value.mutation, output=result.lane)
                    context.delta_mutations[0] = mutation
                    value = DeltaMutationResult(mutation)
                else:
                    if result.binding != "delta_table" or value.mutation.kind != "merge":
                        raise TypeError("A typed same-schema Delta result requires delta_merge(...).execute() on delta_table(...)")
                    target = next(
                        (
                            item
                            for item in request.inputs
                            if item.source == value.mutation.target and item.binding == "delta_table"
                        ),
                        None,
                    )
                    if target is None or target.schema is not result.schema:
                        actual = getattr(target.schema, "__name__", str(target.schema)) if target is not None else "unbound relation"
                        raise TypeError(
                            f"Delta merge result Schema {getattr(result.schema, '__name__', result.schema)} is incompatible with "
                            f"target Schema {actual}"
                        )
                    context.delta_mutations[0] = replace(value.mutation, output_schema=cast(type, result.schema))
                    value = DeltaMutationResult(context.delta_mutations[0])
            else:
                raise TypeError(f"Delta effect step {request.name} must return None or a Delta mutation result")
            results = (PySparkResultBody(),)
            if context.foreach:
                raise TypeError("foreach(row, sink) cannot be used in a Delta effect step")
        else:
            if any(
                mutation.kind not in {"delta_snapshot", "delta_changes", "delta_history", "delta_detail"}
                for mutation in context.delta_mutations
            ):
                raise TypeError("Delta mutations require a None-returning @step bound to delta_output(...)")
            state_operations = [
                operation.stateful_transform
                for operation in context.operations
                if operation.kind == "transform_with_state" and operation.stateful_transform is not None
            ]
            if state_operations:
                if len(request.results) != 1 or len(state_operations) != 1:
                    raise TypeError("transform_with_state(...) requires exactly one declared step output.")
                state_plan = state_operations[0]
                if request.results[0].schema is not state_plan.output_schema:
                    raise TypeError(
                        "transform_with_state processor output Schema must match the step's declared output Schema."
                    )
                if context.filters or context.joins or context.foreach or len(context.operations) != 1:
                    raise TypeError("transform_with_state(...) must be the only operation in its step.")
                from structure.plugin.pyspark.dsl.Stateful import StatefulResult

                if not isinstance(value, StatefulResult) or value.output_schema is not state_plan.output_schema:
                    raise TypeError("A transform_with_state step must return its StatefulResult directly.")
                results = (PySparkResultBody(),)
            else:
                results = BuildPySparkResultBodies(request)(value, context=context)
        sink_captures = self._sink_captures(value, context.foreach, request)
        first = results[0]
        if first.aggregate is not None:
            context.record_aggregate(first.aggregate, context.aggregate_state_budget)
        elif context.aggregate_state_budget is not None:
            raise TypeError("budget(...) after group_by(...) requires an aggregate result in the same step")
        operations = tuple(
            replace(operation, source_span=request.primary_span) if request.primary_span is not None else operation
            for operation in context.operations
        )
        body = PySparkStepBody(
            value=value,
            filters=tuple(context.filters),
            joins=tuple(context.joins),
            operations=operations,
            delta_mutations=tuple(context.delta_mutations),
            aggregate_keys=context.aggregate_keys,
            aggregate_levels=context.aggregate_levels,
            aggregate_grouping=context.aggregate_grouping,
            aggregate_having=context.aggregate_having,
            projection=first.projection,
            aggregate=first.aggregate,
            results=results,
            sinks=sink_captures,
        )
        if not request.sink_effect:
            ValidatePySparkAggregationUse()(body, request=request)
            ValidatePySparkAggregates()(body, request=request)
        ValidatePySparkComparisons()(self._expressions(body), request=request)
        ValidatePySparkRelationReads()(body, request=request)
        return body

    def _sink_captures(self, value: object, captures: list, request: StepAuthoringRequest) -> tuple[StepSinkCapture, ...]:
        if not captures:
            return ()
        values = () if request.sink_effect else ValidatePySparkResultReturn(request, self._raise)(value)
        declared = {sink.name for sink in request.sinks}
        result: list[StepSinkCapture] = []
        for capture in captures:
            sink_name = getattr(capture.sink, "name", None)
            if sink_name not in declared:
                raise TypeError(f"foreach(row, sink) references undeclared sink {sink_name!r} in step {request.name}.")
            ordinal = next((index for index, candidate in enumerate(values) if capture.row is candidate), None)
            input_ordinal = None
            if request.sink_effect and isinstance(capture.row, RowScope):
                input_ordinal = next(
                    (
                        index
                        for index, binding in enumerate(request.inputs)
                        if binding.scope == capture.row._structure_scope_name
                        and binding.schema is capture.row._structure_scope_schema
                    ),
                    None,
                )
            if isinstance(capture.row, RowScope):
                row_schema = capture.row._structure_scope_schema.__name__
            elif isinstance(capture.row, Projection):
                row_schema = capture.row.target.__name__ if capture.row.target is not None else "projection"
            else:
                row_schema = type(capture.row).__name__
            result.append(
                StepSinkCapture(
                    sink=sink_name,
                    result_ordinal=ordinal,
                    row_schema=row_schema,
                    input_ordinal=input_ordinal,
                    kind=capture.kind,
                )
            )
        return tuple(result)

    @staticmethod
    def _raise(code: str, problem: str, use: str) -> None:
        raise TypeError(f"{code}: {problem} {use}")

    def _expressions(self, body: PySparkStepBody) -> tuple[Expression, ...]:
        expressions: list[Expression] = [*body.filters, *(assignment.expression for assignment in body.projection)]
        expressions.extend(
            operation.posexplode_struct.expression
            for operation in body.operations
            if operation.posexplode_struct is not None
        )
        expressions.extend(
            operation.stateful_transform.key
            for operation in body.operations
            if operation.stateful_transform is not None
        )
        expressions.extend(
            operation.json_tuple.expression for operation in body.operations if operation.json_tuple is not None
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
        for operation in body.operations:
            if operation.stateful_transform is not None:
                expressions.append(operation.stateful_transform.key)
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
