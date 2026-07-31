from dataclasses import replace
from typing import Any, cast

from structure.plugin.api.v1.model import BackendCapabilities, CapabilityRequirement, StepPlan
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkGenerator import MapPySparkGenerator
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkHook import MapPySparkHook
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkName import MapPySparkName
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkValidation import MapPySparkValidation
from structure.plugin.pyspark.compiler.model.PySparkAggregateAssignment import PySparkAggregateAssignment
from structure.plugin.pyspark.compiler.model.PySparkAggregateKey import PySparkAggregateKey
from structure.plugin.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.plugin.pyspark.compiler.model.PySparkCacheRecipe import PySparkCacheRecipe
from structure.plugin.pyspark.compiler.model.PySparkDuplicateRowsRecipe import PySparkDuplicateRowsRecipe
from structure.plugin.pyspark.compiler.model.PySparkExactlyOneRecipe import PySparkExactlyOneRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinAsOfRecipe import PySparkJoinAsOfRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinDedupeRecipe import PySparkJoinDedupeRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinTemporalRecipe import PySparkJoinTemporalRecipe
from structure.plugin.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.plugin.pyspark.compiler.model.PySparkOrderedTimelineScanRecipe import PySparkOrderedTimelineScanRecipe
from structure.plugin.pyspark.compiler.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationAliasRecipe import PySparkRelationAliasRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationAssertionRecipe import PySparkRelationAssertionRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationBoundRecipe import PySparkRelationBoundRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationHierarchyClosureRecipe import (
    PySparkRelationHierarchyClosureRecipe,
)
from structure.plugin.pyspark.compiler.model.PySparkRelationHierarchyFallbackRecipe import (
    PySparkRelationHierarchyFallbackRecipe,
)
from structure.plugin.pyspark.compiler.model.PySparkRelationOrderRecipe import PySparkRelationOrderRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationPrioritySelectionRecipe import (
    PySparkRelationPrioritySelectionRecipe,
)
from structure.plugin.pyspark.compiler.model.PySparkRelationSampleRecipe import PySparkRelationSampleRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationSetRecipe import PySparkRelationSetRecipe
from structure.plugin.pyspark.compiler.model.PySparkSelectedRowsRecipe import PySparkSelectedRowsRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepResultRecipe import PySparkStepResultRecipe
from structure.plugin.pyspark.compiler.model.PySparkWatermarkRecipe import PySparkWatermarkRecipe
from structure.plugin.pyspark.dsl.aggregation import AggregatePlan, ProjectAssignment
from structure.plugin.pyspark.dsl.joins import JoinMethod, JoinPlan
from structure.plugin.pyspark.dsl.operations import OperationCapability
from structure.plugin.pyspark.symbolic_execution.model.PySparkResultBody import PySparkResultBody
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class MapPySparkStep:

    def __init__(self) -> None:
        self._names = MapPySparkName()
        self._expressions = MapPySparkExpression()
        self._generators = MapPySparkGenerator(self._expressions)
        self._hooks = MapPySparkHook()
        self._validations = MapPySparkValidation()

    def map(
        self,
        step: StepPlan,
        *,
        last: bool,
        capabilities: BackendCapabilities,
        check_intermediate: bool = True,
    ) -> PySparkStepRecipe:
        body = self._body(step)
        input_alias = self._names.alias(step.input_schema.__name__)
        output_alias = self._names.alias(step.output_schema.__name__)
        operations = self._operations(body, input_alias=input_alias, capabilities=capabilities)
        alias_scopes = self._alias_scopes(body)
        joins = tuple(operation.join for operation in operations if operation.join is not None) or tuple(
            self._join(
                join,
                occurrence=occurrence,
                left_alias=input_alias,
                alias_scopes=alias_scopes,
                capabilities=capabilities,
            )
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
                validations=self._validations.result(result, last=last, check_intermediate=check_intermediate),
                aggregate=(
                    None
                    if body_result.aggregate is None
                    else self._aggregate(body_result.aggregate, capabilities=capabilities)
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
            validations=self._validations.step(step, last=last, check_intermediate=check_intermediate),
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
        alias_scopes = self._alias_scopes(body)
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
                                operation.join,
                                occurrence=occurrence,
                                left_alias=input_alias,
                                alias_scopes=alias_scopes,
                                capabilities=capabilities,
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
                                order_by=self._expressions.map(
                                    operation.selected_rows.order_by, capabilities=capabilities
                                ),
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
                                    self._expressions.map(expression, capabilities=capabilities)
                                    for expression in subset
                                ),
                                scope=None if duplicate_rows is None else duplicate_rows.scope,
                                within_watermark=False if duplicate_rows is None else duplicate_rows.within_watermark,
                            )
                        ),
                        operation,
                    )
                )
            if operation.kind == "exactly_one" and operation.exactly_one is not None:
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.exactly_one_operation(
                            PySparkExactlyOneRecipe(scope=operation.exactly_one.scope)
                        ),
                        operation,
                    )
                )
            if operation.kind == "posexplode_struct" and operation.posexplode_struct is not None:
                recipes.append(
                    self._operation_modes(
                        self._generators.posexplode_struct(
                            operation.posexplode_struct,
                            capabilities=capabilities,
                        ),
                        operation,
                    )
                )
            if operation.kind == "posexplode_outer_struct" and operation.posexplode_struct is not None:
                recipes.append(
                    self._operation_modes(
                        self._generators.posexplode_struct(
                            operation.posexplode_struct,
                            capabilities=capabilities,
                        ),
                        operation,
                    )
                )
            if operation.kind == "explode_struct" and operation.posexplode_struct is not None:
                recipes.append(
                    self._operation_modes(
                        self._generators.posexplode_struct(
                            operation.posexplode_struct,
                            capabilities=capabilities,
                        ),
                        operation,
                    )
                )
            if operation.kind == "explode_outer_struct" and operation.posexplode_struct is not None:
                recipes.append(
                    self._operation_modes(
                        self._generators.posexplode_struct(
                            operation.posexplode_struct,
                            capabilities=capabilities,
                        ),
                        operation,
                    )
                )
            if operation.kind == "inline_struct" and operation.posexplode_struct is not None:
                recipes.append(
                    self._operation_modes(
                        self._generators.posexplode_struct(
                            operation.posexplode_struct,
                            capabilities=capabilities,
                        ),
                        operation,
                    )
                )
            if operation.kind == "inline_outer_struct" and operation.posexplode_struct is not None:
                recipes.append(
                    self._operation_modes(
                        self._generators.posexplode_struct(
                            operation.posexplode_struct,
                            capabilities=capabilities,
                        ),
                        operation,
                    )
                )
            if operation.kind == "ordered_timeline_scan" and operation.ordered_timeline_scan is not None:
                scan = operation.ordered_timeline_scan
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.ordered_timeline_scan_operation(
                            PySparkOrderedTimelineScanRecipe(
                                scope=scan.scope,
                                state_scope=scan.state_scope,
                                row_scope=scan.row_scope,
                                state_schema=scan.state_schema,
                                initial=tuple(
                                    (name, self._expressions.map(expression, capabilities=capabilities))
                                    for name, expression in scan.initial
                                ),
                                transition=tuple(
                                    (name, self._expressions.map(expression, capabilities=capabilities))
                                    for name, expression in scan.transition
                                ),
                                partition_by=tuple(
                                    self._expressions.map(expression, capabilities=capabilities)
                                    for expression in scan.partition_by
                                ),
                                order_by=tuple(
                                    self._expressions.map(expression, capabilities=capabilities)
                                    for expression in scan.order_by
                                ),
                                max_rows=scan.max_rows,
                                ties=scan.ties,
                            )
                        ),
                        operation,
                    )
                )
            if operation.relation_alias is not None:
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.relation_alias_operation(
                            PySparkRelationAliasRecipe(
                                input_name=operation.relation_alias.input_name,
                                source=operation.relation_alias.source,
                                schema=operation.relation_alias.schema,
                                alias=operation.relation_alias.alias,
                            )
                        ),
                        operation,
                    )
                )
            if operation.relation_assertion is not None:
                assertion = operation.relation_assertion
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.relation_assertion_operation(
                            PySparkRelationAssertionRecipe(
                                operation=assertion.operation,
                                keys=tuple(
                                    self._expressions.map(expression, capabilities=capabilities)
                                    for expression in assertion.keys
                                ),
                                predicate=(
                                    None
                                    if assertion.predicate is None
                                    else self._expressions.map(assertion.predicate, capabilities=capabilities)
                                ),
                                value=(
                                    None
                                    if assertion.value is None
                                    else self._expressions.map(assertion.value, capabilities=capabilities)
                                ),
                                reference_input=assertion.reference_input,
                                reference_source=assertion.reference_source,
                                reference_schema=assertion.reference_schema,
                                reference_key=(
                                    None
                                    if assertion.reference_key is None
                                    else self._expressions.map(assertion.reference_key, capabilities=capabilities)
                                ),
                                parent=(
                                    None
                                    if assertion.parent is None
                                    else self._expressions.map(assertion.parent, capabilities=capabilities)
                                ),
                                order_by=(
                                    None
                                    if assertion.order_by is None
                                    else self._expressions.map(assertion.order_by, capabilities=capabilities)
                                ),
                                max_depth=assertion.max_depth,
                                nulls=assertion.nulls,
                            )
                        ),
                        operation,
                    )
                )
            if operation.relation_hierarchy_closure is not None:
                closure = operation.relation_hierarchy_closure
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.relation_hierarchy_closure_operation(
                            PySparkRelationHierarchyClosureRecipe(
                                id=self._expressions.map(closure.id, capabilities=capabilities),
                                parent=self._expressions.map(closure.parent, capabilities=capabilities),
                                schema=closure.schema,
                                scope=closure.scope,
                                node=closure.node,
                                ancestor=closure.ancestor,
                                depth=closure.depth,
                                max_depth=closure.max_depth,
                            )
                        ),
                        operation,
                    )
                )
            if operation.relation_hierarchy_fallback is not None:
                fallback = operation.relation_hierarchy_fallback
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.relation_hierarchy_fallback_operation(
                            PySparkRelationHierarchyFallbackRecipe(
                                source_id=self._expressions.map(fallback.source_id, capabilities=capabilities),
                                path=self._expressions.map(fallback.path, capabilities=capabilities),
                                parent_input=fallback.parent_input,
                                parent_source=fallback.parent_source,
                                parent_schema=fallback.parent_schema,
                                parent_id=self._expressions.map(fallback.parent_id, capabilities=capabilities),
                                parent=self._expressions.map(fallback.parent, capabilities=capabilities),
                                schema=fallback.schema,
                                scope=fallback.scope,
                                source=fallback.source,
                                fallback=fallback.fallback,
                                ordinal=fallback.ordinal,
                                separator=fallback.separator,
                                max_depth=fallback.max_depth,
                            )
                        ),
                        operation,
                    )
                )
            if operation.relation_order is not None:
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.relation_order_operation(
                            PySparkRelationOrderRecipe(
                                order_by=tuple(
                                    self._expressions.map(expression, capabilities=capabilities)
                                    for expression in operation.relation_order.order_by
                                )
                            )
                        ),
                        operation,
                    )
                )
            if operation.relation_bound is not None:
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.relation_bound_operation(
                            operation.kind,
                            PySparkRelationBoundRecipe(count=operation.relation_bound.count),
                        ),
                        operation,
                    )
                )
            if operation.relation_sample is not None:
                sample = operation.relation_sample
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.relation_sample_operation(
                            PySparkRelationSampleRecipe(
                                fraction=sample.fraction,
                                with_replacement=sample.with_replacement,
                                seed=sample.seed,
                                reproducible=sample.reproducible,
                            )
                        ),
                        operation,
                    )
                )
            if operation.relation_priority_selection is not None:
                selection = operation.relation_priority_selection
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.relation_priority_selection_operation(
                            PySparkRelationPrioritySelectionRecipe(
                                keys=tuple(
                                    self._expressions.map(expression, capabilities=capabilities)
                                    for expression in selection.keys
                                ),
                                predicate=self._expressions.map(selection.predicate, capabilities=capabilities),
                                order_by=self._expressions.map(selection.order_by, capabilities=capabilities),
                                missing=selection.missing,
                                ties=selection.ties,
                            )
                        ),
                        operation,
                    )
                )
            if operation.relation_set is not None:
                recipes.append(
                    self._operation_modes(
                        PySparkOperationRecipe.relation_set_operation(
                            PySparkRelationSetRecipe(
                                operation=operation.relation_set.operation,
                                input_name=operation.relation_set.input_name,
                                source=operation.relation_set.source,
                                schema=operation.relation_set.schema,
                                by_name=operation.relation_set.by_name,
                                allow_missing_columns=operation.relation_set.allow_missing_columns,
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
                                expression=self._expressions.map(
                                    operation.watermark.expression, capabilities=capabilities
                                ),
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
            # Compatibility for the transitional Core authorer.
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
        alias_scopes: set[str],
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

        source_name = join.input_name if join.input_name in alias_scopes else self._join_source_name(join.source)
        return PySparkJoinRecipe(
            input_name=join.input_name,
            source=join.source,
            input_schema=join.input_schema,
            left_alias=left_alias,
            right_alias=self._names.join_alias(source_name, occurrence),
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

    def _alias_scopes(self, body: PySparkStepBody) -> set[str]:
        return {
            operation.relation_alias.alias
            for operation in body.operations
            if operation.relation_alias is not None
        }

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
