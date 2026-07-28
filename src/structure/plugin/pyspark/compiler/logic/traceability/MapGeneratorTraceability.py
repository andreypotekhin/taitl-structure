from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapGeneratorTraceability:
    """Map compiler-visible row generators to provenance and static dataflow records."""

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
            if operation.posexplode_struct is not None
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.{operation.kind}[{index}].{operation.posexplode_struct.scope}",
                sources=self._dataflow.reads(operation.posexplode_struct.expression),
                operation=operation.kind,
                step=step.name,
                detail={
                    "scope": operation.posexplode_struct.scope,
                    "schema": operation.posexplode_struct.schema.__name__,
                    "ordinal": operation.posexplode_struct.ordinal,
                },
            )
            for index, operation in enumerate(step.operations)
            if operation.posexplode_struct is not None
        )
