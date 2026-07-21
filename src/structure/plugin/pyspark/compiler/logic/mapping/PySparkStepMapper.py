from dataclasses import replace
from typing import Any, cast

from structure.plugin.api.v1.model import BackendCapabilities, CapabilityRequirement, StepPlan
from structure.plugin.pyspark.compiler.logic.mapping.PySparkExpressionMapper import PySparkExpressionMapper
from structure.plugin.pyspark.compiler.logic.mapping.PySparkHookMapper import PySparkHookMapper
from structure.plugin.pyspark.compiler.logic.mapping.PySparkNameMapper import PySparkNameMapper
from structure.plugin.pyspark.compiler.logic.mapping.PySparkValidationMapper import PySparkValidationMapper
from structure.plugin.pyspark.compiler.model.PySparkAggregateAssignment import PySparkAggregateAssignment
from structure.plugin.pyspark.compiler.model.PySparkAggregateKey import PySparkAggregateKey
from structure.plugin.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.plugin.pyspark.compiler.model.PySparkCacheRecipe import PySparkCacheRecipe
from structure.plugin.pyspark.compiler.model.PySparkDuplicateRowsRecipe import PySparkDuplicateRowsRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinAsOfRecipe import PySparkJoinAsOfRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinDedupeRecipe import PySparkJoinDedupeRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinTemporalRecipe import PySparkJoinTemporalRecipe
from structure.plugin.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.plugin.pyspark.compiler.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.plugin.pyspark.compiler.model.PySparkSelectedRowsRecipe import PySparkSelectedRowsRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepResultRecipe import PySparkStepResultRecipe
from structure.plugin.pyspark.compiler.model.PySparkWatermarkRecipe import PySparkWatermarkRecipe
from structure.plugin.pyspark.dsl.aggregation import AggregatePlan, ProjectAssignment
from structure.plugin.pyspark.dsl.joins import JoinMethod, JoinPlan
from structure.plugin.pyspark.dsl.operations import OperationCapability
from structure.plugin.pyspark.symbolic_execution.model.PySparkResultBody import PySparkResultBody
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class PySparkStepMapper:

    def __init__(self) -> None:
        self._names = PySparkNameMapper()
        self._expressions = PySparkExpressionMapper()
        self._hooks = PySparkHookMapper()
        self._validations = PySparkValidationMapper()

    def map(
        self,
        step: StepPlan,
        *,
        last: bool,
        capabilities: BackendCapabilities,
    ) -> PySparkStepRecipe:
        body = self._body(step)
        input_alias = self._names.alias(step.input_schema.__name__)
        output_alias = self._names.alias(step.output_schema.__name__)
        operations = self._operations(body, input_alias=input_alias, capabilities=capabilities)
        joins = tuple(operation.join for operation in operations if operation.join is not None) or tuple(
            self._join(join, occurrence=occurrence, left_alias=input_alias, capabilities=capabilities)
            for occurrence, join in enumerate(body.joins, start=1)
        )
        results = tuple(
            PySparkStepResultRecipe(
                schema=result.schema,
                lane=result.lane,
                frame=result.frame,
                output_alias=self._names.alias(result.schema.__name__),
                projection=tuple(
                    self._projection(assignment, capabilities=capabilities) for assignment in body_result.projection
                ),
                ordinal=result.ordinal,
                after_hooks=tuple(self._hooks.map(hook) for hook in result.after_hooks),
                validations=self._validations.result(result, last=last),
                aggregate=(
                    None if body_result.aggregate is None else self._aggregate(body_result.aggregate, capabilities=capabilities)
                ),
            )
            for result, body_result in zip(step.results, body.results, strict=True)
        )
        return PySparkStepRecipe(
            name=step.name,
            ordinal=step.ordinal,
            source=step.source,
            source_scope=step.source_scope,
            input_schema=step.input_schema,
            output_schema=step.output_schema,
            input_alias=input_alias,
            output_alias=output_alias,
            before_hooks=tuple(self._hooks.map(hook) for hook in step.before_hooks),
            filters=tuple(self._expressions.map(filter, capabilities=capabilities) for filter in body.filters),
            joins=joins,
            projection=tuple(self._projection(assignment, capabilities=capabilities) for assignment in body.projection),
            after_hooks=tuple(self._hooks.map(hook) for hook in step.after_hooks),
            validations=self._validations.step(step, last=last),
            aggregate=None if body.aggregate is None else self._aggregate(body.aggregate, capabilities=capabilities),
            results=results,
            operations=operations,
            origin=step.origin,
        )

    def _operations(
        self,
        body: PySparkStepBody,
        *,
        input_alias: str,
        capabilities: BackendCapabilities,
    ) -> tuple[PySparkOperationRecipe, ...]:
        recipes: list[PySparkOperationRecipe] = []
        occurrence = 0
        for operation in body.operations:
            self._require_operation_capability(operation.capability, capabilities=capabilities)
            if operation.kind == "filter" and operation.filter is not None:
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.filter_operation(
                            self._expressions.map(operation.filter, capabilities=capabilities)
                        ),
                        operation,
                    )
                )
            if operation.kind == "join" and operation.join is not None:
                occurrence += 1
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.join_operation(
                            self._join(
                                operation.join, occurrence=occurrence, left_alias=input_alias, capabilities=capabilities
                            )
                        ),
                        operation,
                    )
                )
            if operation.kind == "aggregate" and operation.aggregate is not None:
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.aggregate_operation(
                            self._aggregate(operation.aggregate, capabilities=capabilities)
                        ),
                        operation,
                    )
                )
            if operation.kind == "selected_rows" and operation.selected_rows is not None:
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.selected_rows_operation(
                            PySparkSelectedRowsRecipe(
                                direction=operation.selected_rows.direction,
                                order_by=self._expressions.map(operation.selected_rows.order_by, capabilities=capabilities),
                                partition_by=tuple(
                                    self._expressions.map(expression, capabilities=capabilities)
                                    for expression in operation.selected_rows.partition_by
                                ),
                                ties=operation.selected_rows.ties,
                            )
                        ),
                        operation,
                    )
                )
            if operation.kind == "drop_duplicates":
                duplicate_rows = operation.duplicate_rows
                subset = () if duplicate_rows is None else duplicate_rows.subset
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.drop_duplicates_operation(
                            PySparkDuplicateRowsRecipe(
                                subset=tuple(
                                    self._expressions.map(expression, capabilities=capabilities) for expression in subset
                                ),
                                scope=None if duplicate_rows is None else duplicate_rows.scope,
                                within_watermark=False if duplicate_rows is None else duplicate_rows.within_watermark,
                            )
                        ),
                        operation,
                    )
                )
            if operation.kind == "watermark" and operation.watermark is not None:
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.watermark_operation(
                            PySparkWatermarkRecipe(
                                expression=self._expressions.map(operation.watermark.expression, capabilities=capabilities),
                                delay=operation.watermark.delay,
                            )
                        ),
                        operation,
                    )
                )
            if operation.kind == "cache":
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.cache_operation(
                            PySparkCacheRecipe(
                                storage_level=None if operation.cache is None else operation.cache.storage_level
                            )
                        ),
                        operation,
                    )
                )
        return tuple(recipes)

    @staticmethod
    def _operation_modes(recipe: PySparkOperationRecipe, operation) -> PySparkOperationRecipe:
        return replace(recipe, streaming_output_modes=operation.streaming_output_modes)

    def _body(self, step: StepPlan) -> PySparkStepBody:
        body = step.plugin_body
        if body is None:
            # Compatibility for the deprecated Core compile_transform() helper.
            # Core compilation always supplies the opaque body.
            legacy = cast(Any, step)
            return PySparkStepBody(
                value=None,
                filters=legacy.filters,
                joins=legacy.joins,
                operations=legacy.operations,
                projection=legacy.projection,
                aggregate=legacy.aggregate,
                results=tuple(
                    PySparkResultBody(
                        projection=cast(Any, result).projection,
                        aggregate=cast(Any, result).aggregate,
                    )
                    for result in step.results
                ),
            )
        if not isinstance(body, PySparkStepBody):
            raise ValueError(f"PySpark step {step.name!r} has an invalid authoring body.")
        if len(body.results) != len(step.results):
            raise ValueError(f"PySpark step {step.name!r} body has mismatched result count.")
        return body

    def _aggregate(
        self,
        aggregate: AggregatePlan,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkAggregateRecipe:
        capabilities.require(CapabilityRequirement(group="aggregate", name=aggregate.grouping))
        assignments: list[PySparkAggregateAssignment] = []
        for assignment in aggregate.assignments:
            if assignment.function != "key":
                capabilities.require(CapabilityRequirement(group="aggregate", name=assignment.function))
            if assignment.filter is not None:
                capabilities.require(CapabilityRequirement(group="aggregate", name="filtered_metric"))
            assignments.append(
                PySparkAggregateAssignment(
                    field=assignment.field,
                    function=assignment.function,
                    expression=(
                        None
                        if assignment.expression is None
                        else self._expressions.map(assignment.expression, capabilities=capabilities)
                    ),
                    key=assignment.key,
                    arguments=tuple(
                        self._expressions.map(argument, capabilities=capabilities) for argument in assignment.arguments
                    ),
                    filter=(
                        None
                        if assignment.filter is None
                        else self._expressions.map(assignment.filter, capabilities=capabilities)
                    ),
                    order_by=(
                        None
                        if assignment.order_by is None
                        else self._expressions.map(assignment.order_by, capabilities=capabilities)
                    ),
                    options=assignment.options,
                )
            )
        if aggregate.having is not None:
            capabilities.require(CapabilityRequirement(group="aggregate", name="having"))
        return PySparkAggregateRecipe(
            keys=tuple(
                PySparkAggregateKey(
                    name=key.name,
                    expression=self._expressions.map(key.expression, capabilities=capabilities),
                )
                for key in aggregate.keys
            ),
            assignments=tuple(assignments),
            grouping=aggregate.grouping,
            levels=aggregate.levels,
            having=(
                None if aggregate.having is None else self._expressions.map(aggregate.having, capabilities=capabilities)
            ),
        )

    def _require_operation_capability(
        self,
        capability: OperationCapability | None,
        *,
        capabilities: BackendCapabilities,
    ) -> None:
        if capability is not None:
            capabilities.require(
                CapabilityRequirement(
                    group=capability.group,
                    name=capability.name,
                    source=capability.source,
                    docs=capability.docs,
                )
            )

    def _join(
        self,
        join: JoinPlan,
        *,
        occurrence: int,
        left_alias: str,
        capabilities: BackendCapabilities,
    ) -> PySparkJoinRecipe:
        capabilities.require(CapabilityRequirement(group="join", name=join.method.value))
        capabilities.require(CapabilityRequirement(group="join", name=self._join_mode_capability(join)))
        if join.method is JoinMethod.ROWSET:
            self._require_rowset_predicate_capabilities(join, capabilities=capabilities)
        if join.hint is not None:
            capabilities.require(CapabilityRequirement(group="join", name=f"{join.hint.value}_hint"))
        if join.strategy is not None:
            capabilities.require(CapabilityRequirement(group="join", name=f"strategy_{join.strategy.hint()}"))
        dedupe = self._dedupe(join, capabilities=capabilities)
        temporal = self._temporal(join, capabilities=capabilities)
        as_of = self._as_of(join, capabilities=capabilities)

        return PySparkJoinRecipe(
            input_name=join.input_name,
            source=join.source,
            input_schema=join.input_schema,
            left_alias=left_alias,
            right_alias=self._names.join_alias(self._join_source_name(join.source), occurrence),
            how=join.how,
            hint=join.hint,
            strategy=join.strategy,
            predicate=self._expressions.map(join.predicate, capabilities=capabilities),
            occurrence=occurrence,
            method=join.method,
            dedupe=dedupe,
            temporal=temporal,
            as_of=as_of,
        )

    def _dedupe(
        self,
        join: JoinPlan,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkJoinDedupeRecipe | None:
        if join.dedupe is None:
            return None
        capabilities.require(CapabilityRequirement(group="join", name="lookup_dedupe"))
        return PySparkJoinDedupeRecipe(
            order_by=self._expressions.map(join.dedupe.order_by, capabilities=capabilities),
            direction=join.dedupe.direction,
            ties=join.dedupe.ties,
        )

    def _temporal(
        self,
        join: JoinPlan,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkJoinTemporalRecipe | None:
        if join.temporal is None:
            return None
        capabilities.require(CapabilityRequirement(group="join", name="temporal_one"))
        return PySparkJoinTemporalRecipe(
            at=self._expressions.map(join.temporal.at, capabilities=capabilities),
            valid_from=self._expressions.map(join.temporal.valid_from, capabilities=capabilities),
            valid_to=self._expressions.map(join.temporal.valid_to, capabilities=capabilities),
            overlaps=join.temporal.overlaps,
        )

    def _as_of(
        self,
        join: JoinPlan,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkJoinAsOfRecipe | None:
        if join.as_of is None:
            return None
        capabilities.require(CapabilityRequirement(group="join", name="as_of_one"))
        return PySparkJoinAsOfRecipe(
            left_time=self._expressions.map(join.as_of.left_time, capabilities=capabilities),
            right_time=self._expressions.map(join.as_of.right_time, capabilities=capabilities),
            direction=join.as_of.direction,
            tolerance=(
                None
                if join.as_of.tolerance is None
                else self._expressions.map(join.as_of.tolerance, capabilities=capabilities)
            ),
            ties=join.as_of.ties,
        )

    def _join_mode_capability(self, join: JoinPlan) -> str:
        if join.method is JoinMethod.EXISTS:
            return "left_semi_join"
        if join.method is JoinMethod.NOT_EXISTS:
            return "left_anti_join"
        return f"{join.how.value}_join"

    def _require_rowset_predicate_capabilities(self, join: JoinPlan, *, capabilities: BackendCapabilities) -> None:
        if self._has_disjunction(join.predicate):
            capabilities.require(CapabilityRequirement(group="join", name="disjunctive_condition"))
        if self._has_non_equi_condition(join.predicate):
            capabilities.require(CapabilityRequirement(group="join", name="non_equi_condition"))

    def _has_disjunction(self, expression) -> bool:
        return expression.kind == "or" or any(self._has_disjunction(argument) for argument in expression.args)

    def _has_non_equi_condition(self, expression) -> bool:
        if expression.kind in {"gt", "lt", "le", "ge", "ne"}:
            return True
        return any(self._has_non_equi_condition(argument) for argument in expression.args)

    def _join_source_name(self, source: str) -> str:
        return source.removeprefix("input:")

    def _projection(
        self,
        assignment: ProjectAssignment,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkProjectionRecipe:
        capabilities.require(CapabilityRequirement(group="expression", name="projection"))
        return PySparkProjectionRecipe(
            field=assignment.field,
            expression=self._expressions.map(assignment.expression, capabilities=capabilities),
        )
