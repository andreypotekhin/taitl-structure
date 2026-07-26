from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency, OpaqueBoundary
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapHookTraceability:
    """Map raw-hook boundaries to provenance, dataflow, and opaque-boundary records."""

    def provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        hook: PySparkHookRecipe,
        source_transform: str,
        transform_module: str,
    ) -> CompilerProvenance:
        return CompilerProvenance(
            source=f"source:{source_transform}.{hook.name}",
            ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.hook.{hook.phase}.{hook.name}",
            generated=(
                f"generated:{transform_module}.{plan.transform}Generated.run."
                f"step.{step.ordinal}.{step.name}.hook.{hook.phase}.{hook.name}"
            ),
        )

    def dependency(self, step: PySparkStepRecipe, hook: PySparkHookRecipe) -> DataflowDependency:
        return DataflowDependency(
            target=f"{step.name}.hook.{hook.name}",
            sources=(step.input_schema.__name__ if hook.phase == "before" else step.output_schema.__name__,),
            operation="hook",
            step=step.name,
            detail={
                "phase": hook.phase,
                "project_output": hook.project_output,
                "schema_mode": hook.schema_mode.value,
            },
        )

    def boundary(self, step: PySparkStepRecipe, hook: PySparkHookRecipe, schema: str) -> OpaqueBoundary:
        return OpaqueBoundary(
            step=step.name,
            hook=hook.name,
            phase=hook.phase,
            target=hook.target,
            schema=schema,
            reason="arbitrary PySpark hook body",
        )
