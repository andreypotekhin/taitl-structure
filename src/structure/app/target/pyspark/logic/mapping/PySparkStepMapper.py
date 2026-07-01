from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.model.OperationCapability import OperationCapability
from structure.app.compiler.ir.model.ProjectAssignment import ProjectAssignment
from structure.app.compiler.ir.model.StepPlan import StepPlan
from structure.app.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.app.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.pyspark.logic.mapping.PySparkExpressionMapper import PySparkExpressionMapper
from structure.app.target.pyspark.logic.mapping.PySparkHookMapper import PySparkHookMapper
from structure.app.target.pyspark.logic.mapping.PySparkNameMapper import PySparkNameMapper
from structure.app.target.pyspark.logic.mapping.PySparkValidationMapper import PySparkValidationMapper
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.app.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.target.pyspark.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.target.pyspark.model.PySparkStepResultRecipe import PySparkStepResultRecipe


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
        input_alias = self._names.alias(step.input_schema.__name__)
        output_alias = self._names.alias(step.output_schema.__name__)
        operations = self._operations(step, input_alias=input_alias, capabilities=capabilities)
        joins = tuple(operation.join for operation in operations if operation.join is not None) or tuple(
            self._join(join, occurrence=occurrence, left_alias=input_alias, capabilities=capabilities)
            for occurrence, join in enumerate(step.joins, start=1)
        )
        results = tuple(
            PySparkStepResultRecipe(
                schema=result.schema,
                lane=result.lane,
                frame=result.frame,
                output_alias=self._names.alias(result.schema.__name__),
                projection=tuple(
                    self._projection(assignment, capabilities=capabilities) for assignment in result.projection
                ),
                ordinal=result.ordinal,
                after_hooks=tuple(self._hooks.map(hook) for hook in result.after_hooks),
                validations=self._validations.result(result, last=last),
            )
            for result in step.results
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
            filters=tuple(self._expressions.map(filter, capabilities=capabilities) for filter in step.filters),
            joins=joins,
            projection=tuple(self._projection(assignment, capabilities=capabilities) for assignment in step.projection),
            after_hooks=tuple(self._hooks.map(hook) for hook in step.after_hooks),
            validations=self._validations.step(step, last=last),
            results=results,
            operations=operations,
        )

    def _operations(
        self,
        step: StepPlan,
        *,
        input_alias: str,
        capabilities: BackendCapabilities,
    ) -> tuple[PySparkOperationRecipe, ...]:
        recipes: list[PySparkOperationRecipe] = []
        occurrence = 0
        for operation in step.operations:
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
                        self._join(operation.join, occurrence=occurrence, left_alias=input_alias, capabilities=capabilities)
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

    def _join(
        self,
        join: JoinPlan,
        *,
        occurrence: int,
        left_alias: str,
        capabilities: BackendCapabilities,
    ) -> PySparkJoinRecipe:
        capabilities.require(CapabilityRequirement(group="join", name="join_one"))
        capabilities.require(CapabilityRequirement(group="join", name=f"{join.how.value}_join"))
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
            predicate=self._expressions.map(join.predicate, capabilities=capabilities),
            occurrence=occurrence,
        )

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
