from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapRelationPrioritySelectionTraceability:
    """Map declared-key first-qualified selection to traceability records."""

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
                source=f"source:{source_transform}.{step.name}.select_first_qualified.{index}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.select_first_qualified.{index}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.select_first_qualified.{index}"
                ),
            )
            for index, operation in enumerate(step.operations)
            if operation.relation_priority_selection is not None
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.select_first_qualified[{index}]",
                sources=self._sources(operation.relation_priority_selection),
                operation="select_first_qualified",
                step=step.name,
                detail={
                    "keys": len(operation.relation_priority_selection.keys),
                    "missing": operation.relation_priority_selection.missing,
                    "ties": operation.relation_priority_selection.ties.value,
                    "diagnostic": "REL-E0705",
                },
            )
            for index, operation in enumerate(step.operations)
            if operation.relation_priority_selection is not None
        )

    def _sources(self, selection) -> tuple[str, ...]:
        seen: set[str] = set()
        result: list[str] = []
        for expression in (*selection.keys, selection.predicate, selection.order_by):
            for source in self._dataflow.reads(expression):
                if source not in seen:
                    result.append(source)
                    seen.add(source)
        return tuple(result)
