from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class BuildPySparkLineageDiagnostics:
    """Warn about repeated reuse of an already-expanded relation lineage."""

    def __call__(
        self,
        plan: TransformPlan,
        *,
        enabled: bool,
        execution_plan: PySparkExecutionPlan | None = None,
    ) -> tuple[Diagnostic, ...]:
        if not enabled:
            return ()
        diagnostics: list[Diagnostic] = []
        fusion = next(
            (optimization.detail for optimization in (execution_plan.optimizations if execution_plan else ())
             if optimization.kind == "projection-union-fusion"),
            None,
        )
        for step in plan.steps:
            body = step.plugin_body
            if not isinstance(body, PySparkStepBody):
                continue
            finding = self._finding(step, body)
            if finding is None:
                continue
            owner = getattr(step.origin, "owner", None)
            owner_name = getattr(owner, "__name__", plan.name)
            owner_module = getattr(owner, "__module__", "")
            problem = (
                f"{owner_name}.{step.name} reuses relation {finding} after a self-join or branch has already "
                "expanded its lazy logical lineage."
            )
            use = (
                "Insert checkpoint() or local_checkpoint() before reusing the expanded relation. "
                "cache() and persist() retain reusable data but do not truncate this driver-side logical "
                "lineage. See docs/troubleshooting/memory/spark_driver_heap_oom.gotcha.md."
            )
            context = {"relation": finding, "step": step.name, "lineage_action": "bound-required"}
            if fusion is not None:
                problem += f" Structure applied {fusion}, but the remaining self-join still grows exponentially."
                use = (
                    f"Diminish: {fusion}. This reduces one lineage multiplier but does not bound recursive reuse. "
                    "Bound it by inserting checkpoint() or local_checkpoint() before reusing the expanded relation, "
                    "or remove the recurrence by restructuring the algorithm around a stable base relation. "
                    "cache() and persist() do not truncate this driver-side logical lineage. See "
                    "docs/troubleshooting/memory/spark_driver_heap_oom.gotcha.md."
                )
                context.update({"lineage_action": "diminished-residual-risk", "optimization": fusion})
            diagnostics.append(
                Diagnostic(
                    entry=diagnostic_registry.get("PYSPARK-W2701"),
                    problem=problem,
                    use=use,
                    context=context,
                    source=f"{owner_module}.{owner_name}.{step.name}".strip("."),
                    primary_span=self._span(body),
                )
            )
        return tuple(diagnostics)

    def _finding(self, step, body: PySparkStepBody) -> str | None:
        expanded = False
        self_joins = 0
        source = step.source
        scope = step.source_scope
        for operation in body.operations:
            if operation.kind in {"checkpoint", "local_checkpoint"}:
                expanded = False
                self_joins = 0
                continue
            join = operation.join
            if join is not None and (join.source == source or join.input_name == scope):
                if expanded and self_joins:
                    return scope
                expanded = True
                self_joins += 1
                continue
            relation_set = operation.relation_set
            if relation_set is None or operation.kind not in {"union_all", "union_by_name"}:
                continue
            reuses_current = relation_set.source == source or relation_set.input_name == scope
            if expanded and reuses_current:
                return scope
        return None

    @staticmethod
    def _span(body: PySparkStepBody):
        return next((operation.source_span for operation in body.operations if operation.source_span is not None), None)
