from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapRelationAssertionTraceability:
    """Map relation assertions to provenance and static dataflow records."""

    def __init__(self, dataflow: CompilerDataflowReads) -> None:
        self._dataflow = dataflow

    def provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.{operation.kind}.{index}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.{operation.kind}.{index}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.{operation.kind}.{index}"
                ),
            )
            for index, operation in enumerate(step.operations)
            if operation.exactly_one is not None or operation.relation_assertion is not None
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            self._dependency(step, operation, index)
            for index, operation in enumerate(step.operations)
            if operation.exactly_one is not None or operation.relation_assertion is not None
        )

    def _dependency(self, step: PySparkStepRecipe, operation, index: int) -> DataflowDependency:
        if operation.exactly_one is not None:
            return DataflowDependency(
                target=f"{step.name}.exactly_one[{index}].{operation.exactly_one.scope}",
                sources=(operation.exactly_one.scope,),
                operation="exactly_one",
                step=step.name,
                detail={"scope": operation.exactly_one.scope, "diagnostic": "REL-E0701"},
            )
        assertion = operation.relation_assertion
        assert assertion is not None
        return DataflowDependency(
            target=f"{step.name}.{operation.kind}[{index}]",
            sources=self._sources(assertion),
            operation=operation.kind,
            step=step.name,
            detail=self._detail(assertion),
        )

    def _sources(self, assertion) -> tuple[str, ...]:
        expressions = (
            *assertion.keys,
            assertion.predicate,
            assertion.value,
            assertion.reference_key,
            assertion.parent,
            assertion.order_by,
        )
        seen: set[str] = set()
        result: list[str] = []
        for expression in expressions:
            if expression is None:
                continue
            for source in self._dataflow.reads(expression):
                if source not in seen:
                    result.append(source)
                    seen.add(source)
        return tuple(result)

    def _detail(self, assertion) -> dict[str, object]:
        diagnostic = {
            "require_unique": "REL-E0702",
            "require_all": "REL-E0703",
            "require_reference": "REL-E0704",
            "require_parent_hierarchy": "REL-E0706",
        }[assertion.operation]
        detail: dict[str, object] = {"diagnostic": diagnostic}
        if assertion.keys:
            detail["keys"] = len(assertion.keys)
        if assertion.reference_input is not None:
            detail["reference"] = assertion.reference_input
            detail["nulls"] = assertion.nulls
        if assertion.operation == "require_parent_hierarchy":
            detail["max_depth"] = assertion.max_depth
        return detail
