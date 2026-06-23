from __future__ import annotations

from structure.app.compiler.compileability.streaming_compatibility.logic.model.StreamingFinding import StreamingFinding
from structure.app.compiler.compileability.streaming_compatibility.logic.model.StreamingReport import StreamingReport
from structure.app.compiler.compileability.streaming_compatibility.logic.model.StreamingSupport import StreamingSupport
from structure.app.dsl.logic.model.transforms.Join import Join
from structure.app.target.pyspark.logic.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.logic.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.logic.model.PySparkJoinRecipe import PySparkJoinRecipe


class ClassifyStreamingCompatibility:

    def __call__(self, plan: PySparkExecutionPlan, *, required: bool = False) -> StreamingReport:
        findings: list[StreamingFinding] = []
        for step in plan.steps:
            for join in step.joins:
                findings.extend(self._join(step.name, join))
            for hook in (
                *step.before_hooks,
                *step.after_hooks,
                *(hook for result in step.results for hook in result.after_hooks if len(step.results) > 1),
            ):
                findings.extend(self._hook(step.name, hook))

        return StreamingReport(
            transform=plan.transform,
            support=self._fold(findings),
            required=required,
            findings=tuple(findings),
        )

    def _join(self, step: str, join: PySparkJoinRecipe) -> tuple[StreamingFinding, ...]:
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
