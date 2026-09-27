from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class BuildPySparkLineageDiagnostics:
    """Warn when a PySpark query plan is likely to cost the driver disproportionally."""

    def __call__(
        self,
        plan: TransformPlan,
        *,
        enabled: bool,
        execution_plan: PySparkExecutionPlan | None = None,
        repeat_cost: int = 8,
        fanout_cost: int = 8,
        validation_cost: int = 8,
        output_cost: int = 32,
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
            finding = self._finding(step, body, minimum_cost=repeat_cost)
            if finding is None:
                continue
            owner = getattr(step.origin, "owner", None)
            owner_name = getattr(owner, "__name__", plan.name)
            owner_module = getattr(owner, "__module__", "")
            problem = (
                f"{owner_name}.{step.name} reuses relation {finding} after joins or branches have already "
                "made the Spark query plan expensive to build."
            )
            use = (
                "Insert checkpoint() or local_checkpoint() before reusing the expanded relation. "
                "cache() and persist() retain reusable data but do not shorten this driver-side query plan. See "
                "docs/troubleshooting/memory/spark_driver_heap_oom.gotcha.md."
            )
            context = {"relation": finding, "step": step.name, "lineage_action": "bound-required"}
            if fusion is not None:
                problem += f" Structure applied {fusion}, but the remaining self-join still grows exponentially and makes the query plan expensive."
                use = (
                    f"Diminish: {fusion}. This reduces one lineage multiplier but does not bound recursive reuse. "
                    "Bound it by inserting checkpoint() or local_checkpoint() before reusing the expanded relation, "
                    "or remove the recurrence by restructuring the algorithm around a stable base relation. "
                    "cache() and persist() do not shorten this driver-side query plan. See "
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
        diagnostics.extend(self._fanout_diagnostics(plan, fanout_cost=fanout_cost))
        diagnostics.extend(self._validation_diagnostics(plan, validation_cost=validation_cost))
        diagnostics.extend(self._output_diagnostics(plan, output_cost=output_cost))
        return tuple(diagnostics)

    def _finding(self, step, body: PySparkStepBody, *, minimum_cost: int) -> str | None:
        expanded = False
        self_joins = 0
        cost = 0
        source = step.source
        scope = step.source_scope
        for operation in body.operations:
            if operation.kind in {"checkpoint", "local_checkpoint"}:
                expanded = False
                self_joins = 0
                cost = 0
                continue
            cost += self._operation_cost(operation)
            join = operation.join
            if join is not None and (join.source == source or join.input_name == scope):
                if expanded and self_joins and cost >= minimum_cost:
                    return scope
                expanded = True
                self_joins += 1
                continue
            relation_set = operation.relation_set
            if relation_set is None or operation.kind not in {"union_all", "union_by_name"}:
                continue
            reuses_current = relation_set.source == source or relation_set.input_name == scope
            if expanded and reuses_current and cost >= minimum_cost:
                return scope
        return None

    def _fanout_diagnostics(self, plan: TransformPlan, *, fanout_cost: int) -> tuple[Diagnostic, ...]:
        consumers: dict[str, int] = {}
        for step in plan.steps:
            for binding in getattr(step, "inputs", ()):
                consumers[binding.source] = consumers.get(binding.source, 0) + 1
        for output in getattr(plan, "outputs", ()):
            consumers[output.source] = consumers.get(output.source, 0) + 1
        costs = {
            getattr(step, "output_lane", ""): self._body_cost(step.plugin_body)
            for step in plan.steps
            if isinstance(step.plugin_body, PySparkStepBody) and getattr(step, "output_lane", None)
        }
        findings: list[Diagnostic] = []
        for source, count in consumers.items():
            cost = costs.get(source, 0)
            if count < 2 or cost < fanout_cost:
                continue
            findings.append(
                Diagnostic(
                    entry=diagnostic_registry.get("PYSPARK-W2702"),
                    problem=f"Query plan branch {source} is used by {count} downstream operations after {cost} costly operations in the query plan.",
                    use="Add checkpoint() or local_checkpoint() at the shared branch, or reduce the number of consumers. See docs/troubleshooting/memory/spark_driver_heap_oom.gotcha.md.",
                    context={"relation": source, "consumer_count": str(count), "structural_cost": str(cost)},
                    source=f"{plan.name}.{source}",
                )
            )
        return tuple(findings)

    def _validation_diagnostics(self, plan: TransformPlan, *, validation_cost: int) -> tuple[Diagnostic, ...]:
        findings: list[Diagnostic] = []
        for step in plan.steps:
            body = step.plugin_body
            if not isinstance(body, PySparkStepBody):
                continue
            assertions = sum(operation.kind in {"require_all", "require_unique"} for operation in body.operations)
            cost = assertions * 4
            if cost < validation_cost:
                continue
            findings.append(
                Diagnostic(
                    entry=diagnostic_registry.get("PYSPARK-W2703"),
                    problem=f"{step.name} repeats {assertions} strict query plan checks on one branch.",
                    use="Keep checks at input and output boundaries, or checkpoint before validating a reused branch. See docs/dev/Troubleshooting.md.",
                    context={"step": step.name, "validation_count": str(assertions)},
                    source=f"{plan.name}.{step.name}",
                    primary_span=self._span(body),
                )
            )
        return tuple(findings)

    def _output_diagnostics(self, plan: TransformPlan, *, output_cost: int) -> tuple[Diagnostic, ...]:
        total_cost = sum(self._body_cost(step.plugin_body) for step in plan.steps)
        if total_cost < output_cost:
            return ()
        return tuple(
            Diagnostic(
                entry=diagnostic_registry.get("PYSPARK-W2704"),
                problem=f"Output {output.name} depends on {total_cost} costly query plan operations without enough planning boundaries.",
                use="Split the computation with checkpoint() or local_checkpoint(), or simplify the output path. See docs/troubleshooting/memory/spark_driver_heap_oom.gotcha.md.",
                context={"output": output.name, "structural_cost": str(total_cost)},
                source=f"{plan.name}.{output.name}",
            )
            for output in getattr(plan, "outputs", ())
        )

    @classmethod
    def _body_cost(cls, body: object) -> int:
        if not isinstance(body, PySparkStepBody):
            return 0
        return sum(cls._operation_cost(operation) for operation in body.operations)

    @staticmethod
    def _operation_cost(operation) -> int:
        if operation.kind in {"join", "union_all", "union_by_name", "aggregate", "require_all", "require_unique"}:
            return 4
        if operation.kind in {"checkpoint", "local_checkpoint"}:
            return 0
        return 1

    @staticmethod
    def _span(body: PySparkStepBody):
        return next((operation.source_span for operation in body.operations if operation.source_span is not None), None)
