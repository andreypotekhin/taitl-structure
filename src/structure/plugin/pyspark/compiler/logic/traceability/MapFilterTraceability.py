from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapFilterTraceability:
    """Map compiled filters to their provenance and static dataflow records."""

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
                source=f"source:{source_transform}.{step.name}.filter.{index}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.filter.{index}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.where.{index}"
                ),
            )
            for index, _ in enumerate(step.filters)
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.filter[{index}]",
                sources=self._dataflow.reads(filter),
                operation="filter",
                step=step.name,
                detail={},
            )
            for index, filter in enumerate(step.filters)
        )
