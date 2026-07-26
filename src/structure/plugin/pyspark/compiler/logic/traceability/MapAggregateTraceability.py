from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe


class MapAggregateTraceability:
    """Map compiled aggregates and post-aggregate predicates to traceability records."""

    def __init__(self, dataflow: CompilerDataflowReads) -> None:
        self._dataflow = dataflow

    def provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        if step.aggregate is None:
            return ()
        return tuple(
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.aggregate.{assignment.field.name}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.aggregate.{assignment.field.name}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.aggregate.{assignment.field.name}"
                ),
            )
            for assignment in step.aggregate.assignments
        )

    def having_provenance(
        self,
        plan: PySparkExecutionPlan,
        step: PySparkStepRecipe,
        source_transform: str,
        transform_module: str,
    ) -> tuple[CompilerProvenance, ...]:
        if step.aggregate is None or step.aggregate.having is None:
            return ()
        return (
            CompilerProvenance(
                source=f"source:{source_transform}.{step.name}.aggregate.having",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.aggregate.having",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.aggregate.having"
                ),
            ),
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        if step.aggregate is None:
            return ()
        return tuple(
            DataflowDependency(
                target=f"{step.output_schema.__name__}.{assignment.field.name}",
                sources=self._sources(step, assignment),
                operation="aggregate",
                step=step.name,
                detail={
                    "field": assignment.field.name,
                    "function": assignment.function,
                    "key": assignment.key,
                },
            )
            for assignment in step.aggregate.assignments
        )

    def having_dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        if step.aggregate is None or step.aggregate.having is None:
            return ()
        return (
            DataflowDependency(
                target=f"{step.output_schema.__name__}.having",
                sources=self._dataflow.reads(step.aggregate.having),
                operation="having",
                step=step.name,
                detail={"predicate": "post_aggregate"},
            ),
        )

    def _sources(self, step: PySparkStepRecipe, assignment) -> tuple[str, ...]:
        sources: list[str] = []
        expressions = (
            assignment.expression,
            *assignment.arguments,
            assignment.filter,
            assignment.order_by,
        )
        for expression in expressions:
            if expression is None:
                continue
            sources.extend(source for source in self._dataflow.reads(expression) if source not in sources)
        return tuple(sources) or (step.source,)
