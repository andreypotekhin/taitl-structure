from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.compiler.ir.model.OperationCapability import OperationCapability
from structure.app.compiler.ir.model.OutputPlan import OutputPlan
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode
from structure.app.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.app.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.pyspark.logic.mapping.PySparkExpressionMapper import PySparkExpressionMapper
from structure.app.target.pyspark.logic.mapping.PySparkNameMapper import PySparkNameMapper
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.app.target.pyspark.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.app.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
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
        if join.hint is not None:
            capabilities.require(CapabilityRequirement(group="join", name=f"{join.hint.value}_hint"))
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
        )

    def _join_mode_capability(self, join) -> str:
        if join.method is JoinMethod.EXISTS:
            return "left_semi_join"
        if join.method is JoinMethod.NOT_EXISTS:
            return "left_anti_join"
        return f"{join.how.value}_join"

    def _join_source_name(self, source: str) -> str:
        return source.removeprefix("input:")

    def _projection(self, assignment, *, capabilities: BackendCapabilities) -> PySparkProjectionRecipe:
        capabilities.require(CapabilityRequirement(group="expression", name="projection"))
        return PySparkProjectionRecipe(
            field=assignment.field,
            expression=self._expressions.map(assignment.expression, capabilities=capabilities),
        )
