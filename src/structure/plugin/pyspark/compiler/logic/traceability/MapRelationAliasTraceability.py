from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapRelationAliasTraceability:
    """Map relation alias declarations to provenance and static dataflow records."""

    def provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.relation_alias.{index}.{alias.alias}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.relation_alias.{index}.{alias.alias}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.relation_alias.{index}.{alias.alias}"
                ),
            )
            for index, operation in enumerate(step.operations)
            if operation.relation_alias is not None
            for alias in (operation.relation_alias,)
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.relation_alias[{index}].{alias.alias}",
                sources=(alias.source,),
                operation="relation_alias",
                step=step.name,
                detail={
                    "input_name": alias.input_name,
                    "schema": alias.schema.__name__,
                },
            )
            for index, operation in enumerate(step.operations)
            if operation.relation_alias is not None
            for alias in (operation.relation_alias,)
        )
