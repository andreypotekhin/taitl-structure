from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapRelationHierarchyFallbackTraceability:
    """Map hierarchy fallback expansion to provenance and static dataflow records."""

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
                source=f"source:{source_transform}.{step.name}.hierarchy_fallbacks.{index}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.hierarchy_fallbacks.{index}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.hierarchy_fallbacks.{index}"
                ),
            )
            for index, operation in enumerate(step.operations)
            if operation.relation_hierarchy_fallback is not None
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.hierarchy_fallbacks[{index}].{operation.relation_hierarchy_fallback.scope}",
                sources=(
                    *self._dataflow.reads(operation.relation_hierarchy_fallback.source_id),
                    *self._dataflow.reads(operation.relation_hierarchy_fallback.path),
                    *self._dataflow.reads(operation.relation_hierarchy_fallback.parent_id),
                    *self._dataflow.reads(operation.relation_hierarchy_fallback.parent),
                ),
                operation="hierarchy_fallbacks",
                step=step.name,
                detail={
                    "scope": operation.relation_hierarchy_fallback.scope,
                    "schema": operation.relation_hierarchy_fallback.schema.__name__,
                    "parents": operation.relation_hierarchy_fallback.parent_input,
                    "max_depth": operation.relation_hierarchy_fallback.max_depth,
                },
            )
            for index, operation in enumerate(step.operations)
            if operation.relation_hierarchy_fallback is not None
        )
