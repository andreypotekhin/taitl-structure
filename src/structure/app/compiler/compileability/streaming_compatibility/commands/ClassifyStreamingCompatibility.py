from __future__ import annotations

from structure.app.compiler.compileability.streaming_compatibility.model.StreamingFinding import StreamingFinding
from structure.app.compiler.compileability.streaming_compatibility.model.StreamingReport import StreamingReport
from structure.app.compiler.compileability.streaming_compatibility.model.StreamingSupport import StreamingSupport
from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.dsl.model.transforms.Join import Join
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe


class ClassifyStreamingCompatibility:

    def __call__(self, plan: PySparkExecutionPlan, *, required: bool = False) -> StreamingReport:
        findings: list[StreamingFinding] = []
        for step in plan.steps:
            if step.aggregate is not None:
                findings.extend(self._aggregate(step.name))
            findings.extend(self._window_projection(step.name, tuple(assignment.expression for assignment in step.projection)))
            for result in step.results:
                findings.extend(
                    self._window_projection(result.lane, tuple(assignment.expression for assignment in result.projection))
                )
            for operation in step.operations:
                if operation.selected_rows is not None:
                    findings.extend(self._selected_rows(step.name, operation.selected_rows.direction))
                if operation.kind == "drop_duplicates":
                    subset = 0 if operation.duplicate_rows is None else len(operation.duplicate_rows.subset)
                    findings.extend(self._drop_duplicates(step.name, subset=subset))
            for join in step.joins:
                findings.extend(self._join(step.name, join))
            for hook in (
                *step.before_hooks,
                *step.after_hooks,
                *(hook for result in step.results for hook in result.after_hooks if len(step.results) > 1),
            ):
                findings.extend(self._hook(step.name, hook))
        for output in plan.outputs:
            findings.extend(
                self._window_projection(output.name, tuple(assignment.expression for assignment in output.projection))
            )

        return StreamingReport(
            transform=plan.transform,
            support=self._fold(findings),
            required=required,
            findings=tuple(findings),
        )

    def _aggregate(self, step: str) -> tuple[StreamingFinding, ...]:
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation="grouped aggregate",
                problem=(
                    "Grouped aggregations are batch-only until Structure defines streaming output modes, "
                    "watermarks, and state semantics."
                ),
                use="Keep this transform batch-only or move streaming aggregation orchestration outside Structure.",
            ),
        )

    def _selected_rows(self, step: str, direction: str) -> tuple[StreamingFinding, ...]:
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=f"{direction}-row selection",
                problem=(
                    "Selected-row window helpers use ranking over partitions and are batch-only until Structure "
                    "defines streaming state and watermark semantics."
                ),
                use="Keep this transform batch-only or move selected-row streaming state management outside Structure.",
            ),
        )

    def _drop_duplicates(self, step: str, *, subset: int) -> tuple[StreamingFinding, ...]:
        operation = "exact duplicate removal" if not subset else "subset duplicate removal"
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=operation,
                problem=(
                    "Duplicate removal uses DataFrame dropDuplicates() and is batch-only until Structure "
                    "defines streaming state, watermark, and output-mode semantics."
                ),
                use="Keep this transform batch-only or move streaming deduplication orchestration outside Structure.",
            ),
        )

    def _window_projection(self, step: str, expressions: tuple[PySparkExpressionRecipe, ...]) -> tuple[StreamingFinding, ...]:
        if not any(self._has_window(expression) for expression in expressions):
            return ()
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation="window projection",
                problem=(
                    "Ranking, lag, and lead window projections are batch-only until Structure defines streaming "
                    "state and watermark semantics."
                ),
                use="Keep this transform batch-only or move streaming window state management outside Structure.",
            ),
        )

    def _has_window(self, expression: PySparkExpressionRecipe) -> bool:
        data = expression.data or {}
        function = data.get("function")
        return (
            expression.kind == "reserved_v2"
            and isinstance(function, str)
            and function.startswith("window_")
        ) or any(self._has_window(argument) for argument in expression.args)

    def _join(self, step: str, join: PySparkJoinRecipe) -> tuple[StreamingFinding, ...]:
        if join.dedupe is not None:
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation=f"deduped lookup join {join.input_name}",
                    problem=(
                        "Deduped lookup joins use right-side ranking and are batch-only until streaming state "
                        "semantics exist."
                    ),
                    use="Keep this transform batch-only or move the deterministic lookup reduction outside Structure.",
                ),
            )
        if join.temporal is not None:
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation=f"temporal join {join.input_name}",
                    problem=(
                        "Temporal joins depend on validity-window selection and are batch-only until streaming "
                        "state and watermark semantics exist."
                    ),
                    use="Keep this transform batch-only or move the temporal lookup into explicit streaming code.",
                ),
            )
        if join.as_of is not None:
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation=f"as-of join {join.input_name}",
                    problem="As-of joins use candidate ranking and are batch-only until streaming state semantics exist.",
                    use="Keep this transform batch-only or move the as-of lookup into explicit streaming code.",
                ),
            )
        if join.method in {JoinMethod.EXISTS, JoinMethod.NOT_EXISTS}:
            return ()
        if join.how.value in {Join.LEFT.value, "inner"}:
            return ()
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=f"join {join.input_name}",
                problem=(
                    "v1 streaming compatibility supports stream-static left and inner joins only; "
                    f"{join.how.value} joins are batch-only."
                ),
                use="Keep this transform batch-only or rewrite the lookup as a left or inner stream-static join.",
            ),
        )

    def _hook(self, step: str, hook: PySparkHookRecipe) -> tuple[StreamingFinding, ...]:
        if hook.streaming_safe:
            return ()
        return (
            StreamingFinding(
                code="STREAM-W0801",
                support=StreamingSupport.UNKNOWN,
                step=step,
                operation=f"{hook.phase} hook {hook.name}",
                problem="Hooks are arbitrary PySpark code. Structure cannot prove this hook is streaming-compatible.",
                use=(
                    f"Mark {hook.name} with streaming_safe=True only if it avoids Spark actions, "
                    "RDD/Pandas conversion, streaming lifecycle APIs, and stateful streaming operations."
                ),
            ),
        )

    def _fold(self, findings: list[StreamingFinding]) -> StreamingSupport:
        if any(finding.support is StreamingSupport.BATCH_ONLY for finding in findings):
            return StreamingSupport.BATCH_ONLY
        if any(finding.support is StreamingSupport.UNKNOWN for finding in findings):
            return StreamingSupport.UNKNOWN
        return StreamingSupport.COMPATIBLE


classify_streaming_compatibility = ClassifyStreamingCompatibility()
