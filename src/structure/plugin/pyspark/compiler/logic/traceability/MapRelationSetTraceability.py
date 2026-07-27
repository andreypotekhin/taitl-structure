from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapRelationSetTraceability:
    """Map exact-schema relation set operations to provenance and static dataflow records."""

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
            if operation.relation_set is not None
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.{operation.kind}[{index}].{operation.relation_set.input_name}",
                sources=(step.source_scope, operation.relation_set.input_name),
                operation=operation.kind,
                step=step.name,
                detail={
                    "source": operation.relation_set.source,
                    "schema": operation.relation_set.schema.__name__,
                    "by_name": operation.relation_set.by_name,
                },
            )
            for index, operation in enumerate(step.operations)
            if operation.relation_set is not None
        )
