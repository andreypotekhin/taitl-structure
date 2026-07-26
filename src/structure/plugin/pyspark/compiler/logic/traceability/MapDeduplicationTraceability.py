from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapDeduplicationTraceability:
    """Map typed duplicate removal to provenance and static dataflow records."""

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
                source=f"source:{source_transform}.{step.name}.drop_duplicates.{index}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.drop_duplicates.{index}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.drop_duplicates.{index}"
                ),
            )
            for index, operation in enumerate(step.operations)
            if operation.kind == "drop_duplicates"
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.drop_duplicates[{index}]",
                sources=self._sources(operation),
                operation="drop_duplicates",
                step=step.name,
                detail={
                    "scope": self._scope(operation),
                    "subset": str(self._subset_count(operation)),
                },
            )
            for index, operation in enumerate(step.operations)
            if operation.kind == "drop_duplicates"
        )

    def _sources(self, operation) -> tuple[str, ...]:
        duplicate_rows = operation.duplicate_rows
        if duplicate_rows is None or not duplicate_rows.subset:
            return ("current_frame.*",)
        return tuple(source for expression in duplicate_rows.subset for source in self._dataflow.reads(expression))

    def _subset_count(self, operation) -> int:
        duplicate_rows = operation.duplicate_rows
        return 0 if duplicate_rows is None else len(duplicate_rows.subset)

    def _scope(self, operation) -> str:
        duplicate_rows = operation.duplicate_rows
        if duplicate_rows is None or duplicate_rows.scope is None:
            return "current_step_frame"
        return str(duplicate_rows.scope)
