from __future__ import annotations

from structure.dsl import StreamingMode
from structure.plugin.api.v1.model import StreamingFinding, StreamingReport
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.dsl.joins import Join, JoinMethod
from structure.plugin.pyspark.dsl.operations import StreamingSupport


class ClassifyStreamingCompatibility:

    def __call__(self, plan: PySparkExecutionPlan, *, required: bool = False) -> StreamingReport:
        findings: list[StreamingFinding] = []
        input_modes = {input.name: input.streaming for input in plan.inputs}
        watermarks: dict[str, set[str]] = {}
        for step in plan.steps:
            expressions = tuple(assignment.expression for assignment in step.projection)
            findings.extend(self._window_projection(step.name, expressions))
            for result in step.results:
                result_expressions = tuple(assignment.expression for assignment in result.projection)
                findings.extend(self._window_projection(result.lane, result_expressions))
            for operation in step.operations:
                if operation.watermark is not None:
                    watermarks.setdefault(operation.watermark.scope, set()).add(operation.watermark.column)
                    continue
                if operation.aggregate is not None:
                    findings.extend(
                        self._aggregate(
                            step.name,
                            operation.aggregate,
                            watermark_columns=watermarks.get(step.source_scope, set()),
                            scope=step.source_scope,
                        )
                    )
                if operation.selected_rows is not None:
                    findings.extend(self._selected_rows(step.name, operation.selected_rows.direction))
                if operation.kind == "drop_duplicates":
                    subset = 0 if operation.duplicate_rows is None else len(operation.duplicate_rows.subset)
                    findings.extend(
                        self._drop_duplicates(
                            step.name,
                            subset=subset,
                            watermarked=bool(watermarks.get(step.source_scope)),
                            explicit=bool(operation.duplicate_rows and operation.duplicate_rows.within_watermark),
                            streaming_input=input_modes.get(step.source) is StreamingMode.YES,
                        )
                    )
                if operation.join is not None:
                    findings.extend(
                        self._join(
                            step.name,
                            operation.join,
                            input_modes=input_modes,
                            current_input=step.source,
                            current_scope=step.source_scope,
                            watermarks=watermarks,
                        )
                    )
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

    def _aggregate(
        self,
        step: str,
        aggregate,
        *,
        watermark_columns: set[str],
        scope: str,
    ) -> tuple[StreamingFinding, ...]:
        session_keys = tuple(key.expression for key in aggregate.keys if self._is_session_window(key.expression))
        if session_keys:
            return self._session_aggregate(
                step,
                session_keys=session_keys,
                key_count=len(aggregate.keys),
                watermark_columns=watermark_columns,
                scope=scope,
            )
        if not watermark_columns:
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation="grouped aggregate",
                    problem=(
                        "Grouped aggregations on streaming inputs require a compiler-visible watermark and a direct "
                        "event-time grouping key."
                    ),
                    use="Call watermark(event_time_field, delay=...) before group_by(window(event_time_field, ...)) or keep this transform batch-only.",
                ),
            )
        if any(self._watermarked_grouping_key(key.expression, watermark_columns, scope) for key in aggregate.keys):
            return ()
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation="unbounded grouped aggregate",
                problem=(
                    "A watermark alone cannot bound state for a business-key aggregate. The watermark event-time "
                    "field must be grouped directly or through window(event_time, ...)."
                ),
                use="Group by window(the_watermarked_event_time, duration) or the event-time field itself, or keep this transform batch-only.",
            ),
        )

    def _session_aggregate(
        self,
        step: str,
        *,
        session_keys: tuple[PySparkExpressionRecipe, ...],
        key_count: int,
        watermark_columns: set[str],
        scope: str,
    ) -> tuple[StreamingFinding, ...]:
        if key_count < 2:
            return (
                self._session_finding(
                    step,
                    "A streaming session-window aggregate requires an ordinary business grouping key in addition to the session window.",
                    "Group by session_window(the_watermarked_event_time, gap) and at least one business key.",
                ),
            )
        if not watermark_columns or not all(
            self._watermarked_grouping_key(key, watermark_columns, scope) for key in session_keys
        ):
            return (
                self._session_finding(
                    step,
                    "A streaming session-window aggregate requires a preceding watermark on its session event-time field.",
                    "Call watermark(the_session_event_time, delay=...) before group_by(session_window(...), business_key).",
                ),
            )
        return ()

    def _session_finding(self, step: str, problem: str, use: str) -> StreamingFinding:
        return StreamingFinding(
            code="STREAM-E0801",
            support=StreamingSupport.BATCH_ONLY,
            step=step,
            operation="session-window aggregate",
            problem=problem,
            use=use,
        )

    def _watermarked_grouping_key(
        self,
        expression: PySparkExpressionRecipe,
        watermark_columns: set[str],
        scope: str,
    ) -> bool:
        if expression.kind == "field":
            return (
                str(expression.data.get("scope", "")) == scope
                and str(expression.data.get("field", "")) in watermark_columns
            )
        return (
            (expression.kind == "time_window" or (expression.data or {}).get("function") == "session_window")
            and bool(expression.args)
            and expression.args[0].kind == "field"
            and str(expression.args[0].data.get("scope", "")) == scope
            and str(expression.args[0].data.get("field", "")) in watermark_columns
        )

    def _is_session_window(self, expression: PySparkExpressionRecipe) -> bool:
        return (expression.data or {}).get("function") == "session_window"

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

    def _drop_duplicates(
        self,
        step: str,
        *,
        subset: int,
        watermarked: bool,
        explicit: bool,
        streaming_input: bool,
    ) -> tuple[StreamingFinding, ...]:
        if explicit and not streaming_input:
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation="watermark-bounded duplicate removal",
                    problem="drop_duplicates_within_watermark(...) requires an input declared with StreamingMode.YES.",
                    use="Declare the current input with streaming=StreamingMode.YES or use drop_duplicates(...) for cross-mode transforms.",
                ),
            )
        if watermarked:
            return ()
        operation = "exact duplicate removal" if not subset else "subset duplicate removal"
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=operation,
                problem=("Streaming duplicate removal requires a compiler-visible watermark so Spark can bound state."),
                use="Call watermark(event_time_field, delay=...) before drop_duplicates(...) or keep this transform batch-only.",
            ),
        )

    def _window_projection(
        self, step: str, expressions: tuple[PySparkExpressionRecipe, ...]
    ) -> tuple[StreamingFinding, ...]:
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
            expression.kind == "transform_expression" and isinstance(function, str) and function.startswith("window_")
        ) or any(self._has_window(argument) for argument in expression.args)

    def _join(
        self,
        step: str,
        join: PySparkJoinRecipe,
        *,
        input_modes: dict[str, StreamingMode],
        current_input: str,
        current_scope: str,
        watermarks: dict[str, set[str]],
    ) -> tuple[StreamingFinding, ...]:
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
        if input_modes.get(join.source) is StreamingMode.YES:
            return self._stream_stream_join(
                step,
                join,
                input_modes=input_modes,
                current_input=current_input,
                current_scope=current_scope,
                watermarks=watermarks,
            )
        if join.method is JoinMethod.NOT_EXISTS and input_modes.get(current_input) is StreamingMode.YES:
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation=f"stream-static anti join {join.input_name}",
                    problem=(
                        "Stream-static anti joins are not admitted because v4 defines only the left-semi "
                        "existence-filter contract."
                    ),
                    use="Use exists(...) for supported stream-static filtering or keep this transform batch-only.",
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
        if join.method is JoinMethod.ROWSET and self._broad_rowset_join(join):
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation=f"rowset join {join.input_name}",
                    problem=(
                        "Full PySpark rowset joins are batch-only until Structure defines streaming state, "
                        "watermark, and output-mode semantics for broad joins."
                    ),
                    use="Keep this transform batch-only or use a supported stream-static lookup join.",
                ),
            )
        if join.method is JoinMethod.EXISTS:
            return ()
        if join.method is JoinMethod.NOT_EXISTS:
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

    def _stream_stream_join(
        self,
        step: str,
        join: PySparkJoinRecipe,
        *,
        input_modes: dict[str, StreamingMode],
        current_input: str,
        current_scope: str,
        watermarks: dict[str, set[str]],
    ) -> tuple[StreamingFinding, ...]:
        admitted = (
            join.method is JoinMethod.ROWSET and join.how in {Join.INNER, Join.LEFT, Join.RIGHT, Join.FULL}
        ) or join.method is JoinMethod.EXISTS
        if (
            admitted
            and input_modes.get(current_input) is StreamingMode.YES
            and self._watermarked_time_bound(join.predicate, current_scope, join.input_name, watermarks)
        ):
            return ()
        shape = "outer or semi" if admitted else "unsupported"
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=f"stream-stream join {join.input_name}",
                problem=(
                    f"Stream-stream {shape} joins require both inputs declared with StreamingMode.YES, watermarks "
                    "on both bound event-time fields, and an event_time_between(...) constraint."
                ),
                use=(
                    "Declare both inputs with StreamingMode.YES, call watermark(...) for both bound event-time fields, "
                    "and include event_time_between(left_time, right_time, upper=...) in the join predicate."
                ),
            ),
        )

    def _has_event_time_between(self, expression: PySparkExpressionRecipe) -> bool:
        return expression.kind == "event_time_between" or any(
            self._has_event_time_between(argument) for argument in expression.args
        )

    def _watermarked_time_bound(
        self,
        expression: PySparkExpressionRecipe,
        current_scope: str,
        joined_scope: str,
        watermarks: dict[str, set[str]],
    ) -> bool:
        if expression.kind == "event_time_between" and len(expression.args) == 2:
            left, right = expression.args
            return self._watermarked_field(left, current_scope, watermarks) and self._watermarked_field(
                right, joined_scope, watermarks
            )
        return any(
            self._watermarked_time_bound(argument, current_scope, joined_scope, watermarks)
            for argument in expression.args
        )

    def _watermarked_field(
        self,
        expression: PySparkExpressionRecipe,
        scope: str,
        watermarks: dict[str, set[str]],
    ) -> bool:
        return (
            expression.kind == "field"
            and str(expression.data.get("scope", "")) == scope
            and str(expression.data.get("field", "")) in watermarks.get(scope, set())
        )

    def _broad_rowset_join(self, join: PySparkJoinRecipe) -> bool:
        return (
            join.how not in {Join.LEFT, Join.INNER}
            or self._has_disjunction(join.predicate)
            or self._has_non_equi_condition(join.predicate)
        )

    def _has_disjunction(self, expression: PySparkExpressionRecipe) -> bool:
        return expression.kind == "or" or any(self._has_disjunction(argument) for argument in expression.args)

    def _has_non_equi_condition(self, expression: PySparkExpressionRecipe) -> bool:
        if expression.kind in {"gt", "lt", "le", "ge", "ne"}:
            return True
        return any(self._has_non_equi_condition(argument) for argument in expression.args)

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
