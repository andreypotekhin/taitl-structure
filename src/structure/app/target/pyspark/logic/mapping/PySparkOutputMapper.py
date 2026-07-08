from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.compiler.ir.model.OperationCapability import OperationCapability
from structure.app.compiler.ir.model.OutputPlan import OutputPlan
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode
from structure.app.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.app.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.pyspark.logic.mapping.PySparkExpressionMapper import PySparkExpressionMapper
from structure.app.target.pyspark.logic.mapping.PySparkNameMapper import PySparkNameMapper
from structure.app.target.pyspark.model.PySparkDuplicateRowsRecipe import PySparkDuplicateRowsRecipe
from structure.app.target.pyspark.model.PySparkJoinDedupeRecipe import PySparkJoinDedupeRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkJoinTemporalRecipe import PySparkJoinTemporalRecipe
from structure.app.target.pyspark.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.app.target.pyspark.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.app.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.target.pyspark.model.PySparkSelectedRowsRecipe import PySparkSelectedRowsRecipe
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


class PySparkOutputMapper:

    def __init__(self) -> None:
        self._names = PySparkNameMapper()
        self._expressions = PySparkExpressionMapper()

    def map(
        self,
        output: OutputPlan,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkOutputRecipe:
        input_alias = self._names.alias(output.source_schema.__name__)
        output_alias = self._names.alias(output.schema.__name__)
        operations = self._operations(output, input_alias=input_alias, capabilities=capabilities)
        joins = tuple(operation.join for operation in operations if operation.join is not None) or tuple(
            self._join(join, occurrence=occurrence, left_alias=input_alias, capabilities=capabilities)
            for occurrence, join in enumerate(output.joins, start=1)
        )
        return PySparkOutputRecipe(
            name=output.name,
            ordinal=output.ordinal,
            source=output.source,
            source_scope=output.source_scope,
            input_schema=output.source_schema,
            output_schema=output.schema,
            input_alias=input_alias,
            output_alias=output_alias,
            filters=tuple(self._expressions.map(filter, capabilities=capabilities) for filter in output.filters),
            joins=joins,
            projection=tuple(
                self._projection(assignment, capabilities=capabilities) for assignment in output.projection
            ),
            validation=PySparkValidationRecipe(
                target=output.name,
                schema=output.schema,
                mode=SchemaMode.STRICT,
                project=False,
                reason="final",
            ),
            operations=operations,
            aliases=output.aliases,
        )

    def _operations(
        self,
        output: OutputPlan,
        *,
        input_alias: str,
        capabilities: BackendCapabilities,
    ) -> tuple[PySparkOperationRecipe, ...]:
        recipes: list[PySparkOperationRecipe] = []
        occurrence = 0
        for operation in output.operations:
            self._require_operation_capability(operation.capability, capabilities=capabilities)
            if operation.kind == "filter" and operation.filter is not None:
                recipes.append(
                    PySparkOperationRecipe.filter_operation(
                        self._expressions.map(operation.filter, capabilities=capabilities)
                    )
                )
            if operation.kind == "join" and operation.join is not None:
                occurrence += 1
                recipes.append(
                    PySparkOperationRecipe.join_operation(
                        self._join(
                            operation.join, occurrence=occurrence, left_alias=input_alias, capabilities=capabilities
                        )
                    )
                )
            if operation.kind == "selected_rows" and operation.selected_rows is not None:
                recipes.append(
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
                    )
                )
            if operation.kind == "drop_duplicates":
                duplicate_rows = operation.duplicate_rows
                subset = () if duplicate_rows is None else duplicate_rows.subset
                recipes.append(
                    PySparkOperationRecipe.drop_duplicates_operation(
                        PySparkDuplicateRowsRecipe(
                            subset=tuple(
                                self._expressions.map(expression, capabilities=capabilities) for expression in subset
                            )
                        )
                    )
                )
        return tuple(recipes)

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

    def _join(self, join, *, occurrence: int, left_alias: str, capabilities: BackendCapabilities) -> PySparkJoinRecipe:
        capabilities.require(CapabilityRequirement(group="join", name=join.method.value))
        capabilities.require(CapabilityRequirement(group="join", name=self._join_mode_capability(join)))
        if join.method is JoinMethod.ROWSET:
            self._require_rowset_predicate_capabilities(join, capabilities=capabilities)
        if join.hint is not None:
            capabilities.require(CapabilityRequirement(group="join", name=f"{join.hint.value}_hint"))
        dedupe = self._dedupe(join, capabilities=capabilities)
        temporal = self._temporal(join, capabilities=capabilities)
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
        )

    def _dedupe(self, join, *, capabilities: BackendCapabilities) -> PySparkJoinDedupeRecipe | None:
        if join.dedupe is None:
            return None
        capabilities.require(CapabilityRequirement(group="join", name="lookup_dedupe"))
        return PySparkJoinDedupeRecipe(
            order_by=self._expressions.map(join.dedupe.order_by, capabilities=capabilities),
            direction=join.dedupe.direction,
            ties=join.dedupe.ties,
        )

    def _temporal(self, join, *, capabilities: BackendCapabilities) -> PySparkJoinTemporalRecipe | None:
        if join.temporal is None:
            return None
        capabilities.require(CapabilityRequirement(group="join", name="temporal_one"))
        return PySparkJoinTemporalRecipe(
            at=self._expressions.map(join.temporal.at, capabilities=capabilities),
            valid_from=self._expressions.map(join.temporal.valid_from, capabilities=capabilities),
            valid_to=self._expressions.map(join.temporal.valid_to, capabilities=capabilities),
            overlaps=join.temporal.overlaps,
        )

    def _join_mode_capability(self, join) -> str:
        if join.method is JoinMethod.EXISTS:
            return "left_semi_join"
        if join.method is JoinMethod.NOT_EXISTS:
            return "left_anti_join"
        return f"{join.how.value}_join"

    def _require_rowset_predicate_capabilities(self, join, *, capabilities: BackendCapabilities) -> None:
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

    def _projection(self, assignment, *, capabilities: BackendCapabilities) -> PySparkProjectionRecipe:
        capabilities.require(CapabilityRequirement(group="expression", name="projection"))
        return PySparkProjectionRecipe(
            field=assignment.field,
            expression=self._expressions.map(assignment.expression, capabilities=capabilities),
        )
