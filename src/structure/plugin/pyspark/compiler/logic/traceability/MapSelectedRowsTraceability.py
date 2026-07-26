from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapSelectedRowsTraceability:
    """Map selected-row window operations to provenance and static dataflow records."""

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
                source=f"source:{source_transform}.{step.name}.{operation.selected_rows.direction}_by.{index}",
                ir=(
                    f"ir:{plan.transform}.step.{step.ordinal}.{step.name}."
                    f"{operation.selected_rows.direction}_by.{index}"
                ),
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.{operation.selected_rows.direction}_by.{index}"
                ),
            )
            for index, operation in enumerate(step.operations)
            if operation.selected_rows is not None
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.{operation.selected_rows.direction}_by[{index}]",
                sources=self._sources(operation.selected_rows),
                operation=f"{operation.selected_rows.direction}_by",
                step=step.name,
                detail={
                    "direction": operation.selected_rows.direction,
                    "partitions": str(len(operation.selected_rows.partition_by)),
                    "ties": operation.selected_rows.ties.value,
                },
            )
            for index, operation in enumerate(step.operations)
            if operation.selected_rows is not None
        )

    def _sources(self, selected_rows) -> tuple[str, ...]:
        reads = self._dataflow.reads(selected_rows.order_by)
        for expression in selected_rows.partition_by:
            reads = (*reads, *self._dataflow.reads(expression))
        return reads
