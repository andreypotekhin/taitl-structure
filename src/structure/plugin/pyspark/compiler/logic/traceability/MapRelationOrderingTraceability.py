from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapRelationOrderingTraceability:
    """Map relation ordering and bounded-selection operations to traceability records."""

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
            if operation.relation_order is not None or operation.relation_bound is not None
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            self._dependency(step, operation, index)
            for index, operation in enumerate(step.operations)
            if operation.relation_order is not None or operation.relation_bound is not None
        )

    def _dependency(self, step: PySparkStepRecipe, operation, index: int) -> DataflowDependency:
        sources: tuple[str, ...] = (step.source_scope,)
        detail: dict[str, object] = {}
        if operation.relation_order is not None:
            sources = self._reads(*operation.relation_order.order_by)
            detail["order_by"] = len(operation.relation_order.order_by)
        if operation.relation_bound is not None:
            detail["count"] = operation.relation_bound.count
        return DataflowDependency(
            target=f"{step.name}.{operation.kind}[{index}]",
            sources=sources,
            operation=operation.kind,
            step=step.name,
            detail=detail,
        )

    def _reads(self, *expressions) -> tuple[str, ...]:
        seen: set[str] = set()
        result: list[str] = []
        for expression in expressions:
            for source in self._dataflow.reads(expression):
                if source not in seen:
                    result.append(source)
                    seen.add(source)
        return tuple(result)
