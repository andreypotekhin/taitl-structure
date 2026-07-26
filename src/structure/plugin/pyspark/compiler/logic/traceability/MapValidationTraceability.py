from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


class MapValidationTraceability:
    """Map step and final output validation to traceability records."""

    def provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.validation.{index}.{validation.reason}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.validation.{index}.{validation.reason}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.validation.{index}.{validation.reason}"
                ),
            )
            for index, validation in enumerate(step.validations)
        )

    def final_provenance(
        self,
        plan: PySparkExecutionPlan,
        output,
        source_transform: str,
        transform_module: str,
    ) -> CompilerProvenance:
        return CompilerProvenance(
            source=f"source:{source_transform}.output.{output.name}",
            ir=f"ir:{plan.transform}.output.{output.ordinal}.{output.name}.validation.final",
            generated=(
                f"generated:{transform_module}.{plan.transform}Generated.run."
                f"output.{output.ordinal}.{output.name}.validation.final"
            ),
        )

    def final_dependency(self, validation: PySparkValidationRecipe) -> DataflowDependency:
        return DataflowDependency(
            target=f"{validation.schema.__name__}.validation.final",
            sources=(validation.schema.__name__,),
            operation="validate_schema",
            step=None,
            detail={"mode": validation.mode.value},
        )
