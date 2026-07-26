from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapRelationAssertionTraceability:
    """Map relation assertions to provenance and static dataflow records."""

    def provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.exactly_one.{index}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.exactly_one.{index}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.exactly_one.{index}"
                ),
            )
            for index, operation in enumerate(step.operations)
            if operation.exactly_one is not None
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.exactly_one[{index}].{operation.exactly_one.scope}",
                sources=(operation.exactly_one.scope,),
                operation="exactly_one",
                step=step.name,
                detail={"scope": operation.exactly_one.scope, "diagnostic": "REL-E0701"},
            )
            for index, operation in enumerate(step.operations)
            if operation.exactly_one is not None
        )
