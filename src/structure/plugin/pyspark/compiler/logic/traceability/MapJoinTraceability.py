from structure.plugin.api.v1.model import CompilerProvenance, DataflowDependency
from structure.plugin.pyspark.compiler.logic.traceability.CompilerDataflowReads import CompilerDataflowReads
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.dsl.joins import JoinMethod


class MapJoinTraceability:
    """Map compiled joins to their provenance and static dataflow records."""

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
                source=f"source:{source_transform}.{step.name}.join.{join.occurrence}.{join.input_name}",
                ir=f"ir:{plan.transform}.step.{step.ordinal}.{step.name}.join.{join.occurrence}.{join.input_name}",
                generated=(
                    f"generated:{transform_module}.{plan.transform}Generated.run."
                    f"step.{step.ordinal}.{step.name}.join.{join.occurrence}.{join.input_name}"
                ),
            )
            for join in step.joins
        )

    def dependencies(self, step: PySparkStepRecipe) -> tuple[DataflowDependency, ...]:
        return tuple(
            DataflowDependency(
                target=f"{step.name}.join[{join.occurrence}].{join.input_name}",
                sources=self._dataflow.reads(join.predicate),
                operation=join.method.value,
                step=step.name,
                detail=self._detail(join),
            )
            for join in step.joins
        )

    def _detail(self, join) -> dict[str, str | None]:
        detail = {
            "cardinality": self._cardinality(join.method),
            "hint": join.hint.value if join.hint is not None else None,
            "how": join.how.value,
            "right_alias": join.right_alias,
        }
        if join.strategy is not None:
            detail["strategy"] = join.strategy.value
        if join.dedupe is not None:
            detail["dedupe"] = join.dedupe.direction
            detail["ties"] = join.dedupe.ties.value
        if join.temporal is not None:
            detail["temporal"] = "closed_open"
            detail["overlaps"] = join.temporal.overlaps.value
        if join.as_of is not None:
            detail["as_of"] = join.as_of.direction.value
            detail["ties"] = join.as_of.ties.value
        return detail

    def _cardinality(self, method: JoinMethod) -> str:
        if method in {JoinMethod.EXISTS, JoinMethod.NOT_EXISTS}:
            return "row_filtering"
        if method is JoinMethod.ROWSET:
            return "row_multiplying"
        return "select_one"
