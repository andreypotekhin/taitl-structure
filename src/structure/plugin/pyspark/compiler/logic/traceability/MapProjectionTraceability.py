from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapProjectionTraceability:
    """Map ordinary and multi-result projections to traceability records."""

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
                source=f"source:{source_transform}.{step.name}.field.{assignment.field.name}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.project.{assignment.field.name}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.select.{assignment.field.name}"
                ),
            )
            for assignment in step.projection
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.output_schema.__name__}.{assignment.field.name}",
                sources=self._dataflow.reads(assignment.expression) or ("literal",),
                operation="project",
                step=step.name,
                detail={"field": assignment.field.name},
            )
            for assignment in step.projection
        )

    def result_provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        result,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.result.{result.lane}.{assignment.field.name}",
                ir=(
                    f"ir:{plan.transform}.step.{step.ordinal}.{step.name}."
                    f"result.{result.ordinal}.{result.lane}.project.{assignment.field.name}"
                ),
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.{result.lane}.select.{assignment.field.name}"
                ),
            )
            for assignment in result.projection
        )

    def result_dependencies(self, step: PySparkStepRecipe, result) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{result.lane}.{assignment.field.name}",
                sources=self._dataflow.reads(assignment.expression) or ("literal",),
                operation="project",
                step=step.name,
                detail={"field": assignment.field.name, "result": result.lane},
            )
            for assignment in result.projection
        )
