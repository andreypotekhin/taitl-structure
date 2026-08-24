from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.api.v1.model import StreamingFinding, StreamingReport, StreamingStateStage
from structure.plugin.pyspark.compiler.logic.streaming.ClassifyGeneratorStreamingCompatibility import (
    ClassifyGeneratorStreamingCompatibility,
)
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.dsl.joins import Join, JoinMethod
from structure.plugin.pyspark.dsl.operations import StreamingSupport


@dataclass(frozen=True)
class _StatefulStreamingOperation:
    step: str
    operation: str
    aggregate: object | None = None


class ClassifyStreamingCompatibility:

    def __init__(self) -> None:
        self._generators = ClassifyGeneratorStreamingCompatibility()

    def __call__(
        self,
        plan: PySparkExecutionPlan,
        *,
        required: bool = False,
        streaming_contract: bool = False,
    ) -> StreamingReport:
        findings: list[StreamingFinding] = []
        input_modes = {input.name: input.streaming for input in plan.inputs}
        watermarks: dict[str, set[str]] = {}
        watermark_delays: dict[str, dict[str, str]] = {}
        stateful_operations: list[_StatefulStreamingOperation] = []
        state_stages: list[StreamingStateStage] = []
        for step in plan.steps:
            streaming_source = bool(input_modes.get(step.source))
            uses_streaming_input = self._uses_streaming_input(step, input_modes)
            streaming_step = streaming_contract or streaming_source or uses_streaming_input
            expressions = tuple(assignment.expression for assignment in step.projection)
            findings.extend(self._window_projection(step.name, expressions, streaming=streaming_step))
            for result in step.results:
                result_expressions = tuple(assignment.expression for assignment in result.projection)
                findings.extend(self._window_projection(result.lane, result_expressions, streaming=streaming_step))
            for operation in step.operations:
                if streaming_step and operation.kind in {"persist", "unpersist", "checkpoint", "local_checkpoint"}:
                    findings.append(
                        StreamingFinding(
                            code="STREAM-E0801",
                            support=StreamingSupport.BATCH_ONLY,
                            step=step.name,
                            operation=operation.kind,
                            problem=f"{operation.kind}() is only admitted for batch DataFrames.",
                            use="Keep this materialization operation on batch input, or move streaming lifecycle policy to the caller.",
                        )
                    )
                if operation.watermark is not None:
                    if not streaming_step:
                        continue
                    watermarks.setdefault(operation.watermark.scope, set()).add(operation.watermark.column)
                    watermark_delays.setdefault(operation.watermark.scope, {})[
                        operation.watermark.column
                    ] = operation.watermark.delay
                    continue
                if operation.aggregate is not None:
                    allow_chained_window = len(stateful_operations) == 1 and self._approved_chained_window(
                        stateful_operations[0], operation.aggregate
                    )
                    operation_findings = self._aggregate(
                        step.name,
                        operation.aggregate,
                        watermark_columns=watermarks.get(step.source_scope, set()),
                        scope=step.source_scope,
                        streaming_source=streaming_step,
                        allow_chained_window=allow_chained_window,
                    )
                    findings.extend(operation_findings)
                    if streaming_step and not self._has_blocking_findings(operation_findings):
                        state_stages.append(
                            self._aggregate_stage(
                                step.name,
                                operation,
                                operation.aggregate,
                                watermarks=watermarks,
                                watermark_delays=watermark_delays,
                            )
                        )
                        stateful_operations.append(
                            _StatefulStreamingOperation(
                                step.name,
                                self._aggregate_operation(operation.aggregate),
                                aggregate=operation.aggregate,
                            )
                        )
                if streaming_step and operation.selected_rows is not None:
                    findings.extend(self._selected_rows(step.name, operation.selected_rows.direction))
                if operation.kind == "drop_duplicates":
                    subset = 0 if operation.duplicate_rows is None else len(operation.duplicate_rows.subset)
                    operation_findings = self._drop_duplicates(
                        step.name,
                        subset=subset,
                        watermarked=bool(watermarks.get(step.source_scope)),
                        explicit=bool(operation.duplicate_rows and operation.duplicate_rows.within_watermark),
                        streaming_input=streaming_step,
                    )
                    findings.extend(operation_findings)
                    if streaming_step and not operation_findings:
                        state_stages.append(
                            self._dedupe_stage(
                                step.name,
                                operation,
                                operation.duplicate_rows,
                                watermark_delays=watermark_delays,
                                scope=step.source_scope,
                            )
                        )
                        stateful_operations.append(
                            _StatefulStreamingOperation(step.name, "watermark-bounded duplicate removal")
                        )
                if streaming_step and operation.exactly_one is not None:
                    findings.extend(self._exactly_one(step.name, operation.exactly_one.scope))
                if streaming_step and operation.relation_assertion is not None:
                    findings.extend(self._relation_assertion(step.name, operation.kind))
                if streaming_step and operation.posexplode_struct is not None:
                    findings.extend(self._generators.posexplode_struct(step.name, operation.posexplode_struct))
                if operation.scalar_generator is not None:
                    findings.extend(self._generators.scalar_array(step.name, operation.scalar_generator))
                if operation.map_generator is not None:
                    findings.extend(self._generators.map(step.name, operation.map_generator))
                if streaming_step and operation.relation_order is not None:
                    findings.extend(self._relation_ordering(step.name, "order_by"))
                if streaming_step and operation.relation_bound is not None:
                    findings.extend(self._relation_ordering(step.name, operation.kind))
                if streaming_step and operation.relation_sample is not None:
                    findings.extend(self._relation_ordering(step.name, "sample"))
                if streaming_step and operation.relation_priority_selection is not None:
                    findings.extend(self._priority_selection(step.name))
                if streaming_step and operation.relation_hierarchy_closure is not None:
                    findings.extend(self._hierarchy_closure(step.name))
                if streaming_step and operation.relation_hierarchy_fallback is not None:
                    findings.extend(self._hierarchy_fallbacks(step.name))
                if operation.relation_set is not None:
                    input_streaming = bool(input_modes.get(operation.relation_set.source))
                    if not streaming_step and not input_streaming:
                        continue
                    findings.extend(
                        self._relation_set(
                            step.name,
                            operation.kind,
                            operation.relation_set.input_name,
                            operation.relation_set.source,
                            allow_missing_columns=operation.relation_set.allow_missing_columns,
                            current_streaming=streaming_step,
                            input_modes=input_modes,
                        )
                    )
                if operation.join is not None:
                    operation_findings = self._join(
                        step.name,
                        operation.join,
                        input_modes=input_modes,
                        current_input=step.source,
                        current_scope=step.source_scope,
                        watermarks=watermarks,
                    )
                    findings.extend(operation_findings)
                    if not operation_findings and self._admitted_stream_stream_join(
                        operation.join,
                        input_modes=input_modes,
                        current_input=step.source,
                        current_scope=step.source_scope,
                        watermarks=watermarks,
                    ):
                        state_stages.append(
                            self._join_stage(
                                step.name,
                                operation,
                                operation.join,
                                watermark_delays=watermark_delays,
                            )
                        )
                        stateful_operations.append(_StatefulStreamingOperation(step.name, "bounded stream-stream join"))
            for hook in (
                *step.before_hooks,
                *step.after_hooks,
                *(hook for result in step.results for hook in result.after_hooks if len(step.results) > 1),
            ):
                if uses_streaming_input:
                    findings.extend(self._hook(step.name, hook))
            input_modes.setdefault(step.name, streaming_step)
            for result in step.results:
                input_modes.setdefault(result.lane, streaming_step)
        for output in plan.outputs:
            findings.extend(
                self._window_projection(
                    output.name,
                    tuple(assignment.expression for assignment in output.projection),
                    streaming=bool(input_modes.get(output.source)),
                )
            )
        findings.extend(self._stateful_composition(stateful_operations))

        return StreamingReport(
            transform=plan.transform,
            support=self._fold(findings),
            required=required,
            findings=tuple(findings),
            stages=tuple(state_stages),
        )

    def _aggregate_stage(
        self,
        step: str,
        operation,
        aggregate,
        *,
        watermarks: dict[str, set[str]],
        watermark_delays: dict[str, dict[str, str]],
    ) -> StreamingStateStage:
        scope = self._aggregate_scope(aggregate)
        watermark_columns = watermarks.get(scope, set())
        return StreamingStateStage(
            step=step,
            operation=self._aggregate_operation(aggregate),
            event_time=self._aggregate_event_time(aggregate, watermark_columns),
            watermarks=self._watermark_metadata(scope, watermark_delays),
            keys=tuple(key.name for key in aggregate.keys),
            retention=self._aggregate_retention(aggregate),
            order_keys=tuple(
                dict.fromkeys(
                    self._expression_label(assignment.order_by)
                    for assignment in aggregate.assignments
                    if assignment.order_by is not None
                )
            ),
            output_modes=tuple(mode.value for mode in operation.streaming_output_modes),
            allows_later_stateful=any(
                self._is_event_time_window(key.expression) and not self._is_window_time_window(key.expression)
                for key in aggregate.keys
            ),
        )

    def _dedupe_stage(
        self,
        step: str,
        operation,
        duplicate_rows,
        *,
        watermark_delays: dict[str, dict[str, str]],
        scope: str,
    ) -> StreamingStateStage:
        return StreamingStateStage(
            step=step,
            operation="watermark-bounded duplicate removal",
            event_time=tuple(f"{scope}.{column}" for column in sorted(watermark_delays.get(scope, {}))),
            watermarks=self._watermark_metadata(scope, watermark_delays),
            keys=tuple(
                self._expression_label(expression) for expression in (duplicate_rows.subset if duplicate_rows else ())
            ),
            retention=tuple(sorted(watermark_delays.get(scope, {}).values())),
            output_modes=tuple(mode.value for mode in operation.streaming_output_modes),
        )

    def _join_stage(
        self,
        step: str,
        operation,
        join: PySparkJoinRecipe,
        *,
        watermark_delays: dict[str, dict[str, str]],
    ) -> StreamingStateStage:
        event_time = self._join_event_time(join.predicate)
        scopes = tuple(sorted({scope for scope, _ in event_time}))
        metadata = tuple(item for scope in scopes for item in self._watermark_metadata(scope, watermark_delays))
        modes = tuple(mode.value for mode in operation.streaming_output_modes)
        if not modes and (join.method is JoinMethod.EXISTS or join.how in {Join.LEFT, Join.RIGHT, Join.FULL}):
            modes = ("append",)
        return StreamingStateStage(
            step=step,
            operation="bounded stream-stream join",
            event_time=tuple(f"{scope}.{field}" for scope, field in event_time),
            watermarks=metadata,
            keys=tuple(
                self._join_key_label(candidate)
                for candidate in self._walk_expressions(join.predicate)
                if candidate.kind == "eq"
            ),
            retention=self._join_retention(join.predicate),
            output_modes=modes,
        )

    def _aggregate_scope(self, aggregate) -> str:
        for key in aggregate.keys:
            for expression in self._walk_expressions(key.expression):
                scope = str(expression.data.get("scope", ""))
                if scope:
                    return scope
        return ""

    def _aggregate_event_time(self, aggregate, watermark_columns: set[str]) -> tuple[str, ...]:
        values: list[str] = []
        for key in aggregate.keys:
            if self._is_event_time_window(key.expression) or self._is_session_window(key.expression):
                values.extend(self._expression_label(expression) for expression in key.expression.args[:1])
        if values:
            return tuple(dict.fromkeys(values))
        return tuple(
            self._expression_label(expression)
            for key in aggregate.keys
            for expression in self._walk_expressions(key.expression)
            if expression.kind == "field" and str(expression.data.get("field", "")) in watermark_columns
        )

    def _aggregate_retention(self, aggregate) -> tuple[str, ...]:
        values: list[str] = []
        for key in aggregate.keys:
            for expression in self._walk_expressions(key.expression):
                data = expression.data or {}
                if expression.kind == "time_window" and data.get("duration"):
                    values.append(f"window={data['duration']}")
                if data.get("function") == "session_window" and data.get("gap"):
                    values.append(f"session_gap={data['gap']}")
        return tuple(dict.fromkeys(values))

    def _watermark_metadata(
        self,
        scope: str,
        watermark_delays: dict[str, dict[str, str]],
    ) -> tuple[tuple[str, str], ...]:
        return tuple((f"{scope}.{column}", delay) for column, delay in sorted(watermark_delays.get(scope, {}).items()))

    def _join_event_time(self, expression: PySparkExpressionRecipe) -> tuple[tuple[str, str], ...]:
        values: list[tuple[str, str]] = []
        for candidate in self._walk_expressions(expression):
            if candidate.kind != "event_time_between" or len(candidate.args) != 2:
                continue
            for argument in candidate.args:
                if argument.kind == "field":
                    values.append((str(argument.data.get("scope", "")), str(argument.data.get("field", ""))))
        return tuple(dict.fromkeys(values))

    def _join_retention(self, expression: PySparkExpressionRecipe) -> tuple[str, ...]:
        values: list[str] = []
        for candidate in self._walk_expressions(expression):
            if candidate.kind != "event_time_between":
                continue
            data = candidate.data or {}
            if data.get("lower") is not None:
                values.append(f"lower={data['lower']}")
            if data.get("upper") is not None:
                values.append(f"upper={data['upper']}")
        return tuple(dict.fromkeys(values))

    def _join_key_label(self, expression: PySparkExpressionRecipe) -> str:
        if expression.kind == "event_time_between":
            return "event_time_between"
        if expression.kind == "eq" and len(expression.args) == 2:
            return "=".join(self._expression_label(argument) for argument in expression.args)
        if expression.kind == "field":
            return self._expression_label(expression)
        return expression.kind

    def _expression_label(self, expression: PySparkExpressionRecipe) -> str:
        if expression.kind == "field":
            scope = str(expression.data.get("scope", ""))
            field = str(expression.data.get("field", ""))
            return f"{scope}.{field}" if scope else field
        if expression.kind == "window_time" and expression.args:
            return f"window_time({self._expression_label(expression.args[0])})"
        if expression.kind == "time_window" and expression.args:
            duration = str((expression.data or {}).get("duration", ""))
            return f"window({self._expression_label(expression.args[0])}, {duration})"
        if (expression.data or {}).get("function") == "session_window" and expression.args:
            gap = str((expression.data or {}).get("gap", ""))
            return f"session_window({self._expression_label(expression.args[0])}, {gap})"
        return expression.kind

    def _walk_expressions(self, expression: PySparkExpressionRecipe):
        yield expression
        for argument in expression.args:
            yield from self._walk_expressions(argument)

    def _aggregate_operation(self, aggregate) -> str:
        if any(self._is_session_window(key.expression) for key in aggregate.keys):
            return "session-window aggregate"
        if any(self._is_event_time_window(key.expression) for key in aggregate.keys):
            return "watermark-bounded event-time aggregate"
        return "unbounded business-key aggregate"

    def _uses_streaming_input(self, step, input_modes: dict[str, bool]) -> bool:
        return bool(input_modes.get(step.source)) or any(bool(input_modes.get(source)) for source in step.input_sources)

    def _has_blocking_findings(self, findings: tuple[StreamingFinding, ...]) -> bool:
        return any(finding.support is not StreamingSupport.COMPATIBLE for finding in findings)

    def _stateful_composition(
        self,
        operations: list[_StatefulStreamingOperation],
    ) -> tuple[StreamingFinding, ...]:
        if len(operations) <= 1:
            return ()
        if len(operations) == 2 and self._approved_chained_window(operations[0], operations[1].aggregate):
            return ()
        first, second = operations[0], operations[1]
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=second.step,
                operation="stateful streaming composition",
                problem=(
                    "A streaming transform may contain one admitted stateful operation followed only by "
                    f"stateless work; found {first.operation} in {first.step} and {second.operation} in {second.step}."
                ),
                use=(
                    "Keep one watermarked dedupe, window/session aggregate, or bounded stream-stream join in this "
                    "transform, then split any later stateful work into a separate pipeline boundary."
                ),
            ),
        )

    def _aggregate(
        self,
        step: str,
        aggregate,
        *,
        watermark_columns: set[str],
        scope: str,
        streaming_source: bool,
        allow_chained_window: bool = False,
    ) -> tuple[StreamingFinding, ...]:
        if not streaming_source:
            return ()
        session_keys = tuple(key.expression for key in aggregate.keys if self._is_session_window(key.expression))
        if session_keys:
            return self._session_aggregate(
                step,
                session_keys=session_keys,
                key_count=len(aggregate.keys),
                watermark_columns=watermark_columns,
                scope=scope,
            )
        if allow_chained_window and self._is_chained_window_aggregate(aggregate):
            return ()
        event_time_window = any(self._is_event_time_window(key.expression) for key in aggregate.keys)
        if not watermark_columns:
            if self._is_chained_window_aggregate(aggregate):
                return (
                    StreamingFinding(
                        code="STREAM-E0801",
                        support=StreamingSupport.BATCH_ONLY,
                        step=step,
                        operation="chained window aggregate",
                        problem=(
                            "A chained event-time window aggregate requires a preceding admitted window aggregate "
                            "and window_time(...) between the two stateful stages."
                        ),
                        use=(
                            "Keep the first window watermarked, project its TimeWindow value through window_time(...), "
                            "and use that timestamp for the second window, or split the pipeline."
                        ),
                    ),
                )
            if not event_time_window:
                return (self._unbounded_aggregate_warning(step, aggregate),)
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
        if not event_time_window:
            return (self._unbounded_aggregate_warning(step, aggregate),)
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation="grouped aggregate",
                problem=(
                    "An event-time window aggregate must group directly by the watermarked event-time field or by "
                    "window(the_watermarked_event_time, ...)."
                ),
                use="Group by window(the_watermarked_event_time, duration) or the event-time field itself, or keep this transform batch-only.",
            ),
        )

    def _unbounded_aggregate_warning(self, step: str, aggregate) -> StreamingFinding:
        grouping = "business-key" if aggregate.keys else "global"
        return StreamingFinding(
            code="STREAM-W0802",
            support=StreamingSupport.COMPATIBLE,
            step=step,
            operation=f"unbounded {grouping} aggregate",
            problem=(
                "Spark permits this streaming aggregate with update or complete output mode, but its state is not "
                "bounded by a watermark because it does not group by event time."
            ),
            use=(
                "Use an event-time or session window with a matching watermark when state must be bounded, or "
                "explicitly operate the caller-owned query with an accepted unbounded-state policy."
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

    def _approved_chained_window(
        self,
        first: _StatefulStreamingOperation,
        second: object | None,
    ) -> bool:
        return (
            first.aggregate is not None and second is not None and self._is_chained_window_pair(first.aggregate, second)
        )

    def _is_chained_window_pair(self, first, second) -> bool:
        first_window = any(self._is_event_time_window(key.expression) for key in first.keys)
        second_window = any(self._is_window_time_window(key.expression) for key in second.keys)
        return first_window and second_window

    def _is_chained_window_aggregate(self, aggregate) -> bool:
        return any(self._is_window_time_window(key.expression) for key in aggregate.keys)

    def _is_event_time_window(self, expression: PySparkExpressionRecipe) -> bool:
        return expression.kind == "time_window" and bool(expression.args)

    def _is_window_time_window(self, expression: PySparkExpressionRecipe) -> bool:
        return (
            expression.kind == "time_window"
            and bool(expression.args)
            and expression.args[0].kind == "window_time"
            and bool(expression.args[0].args)
        )

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

    def _exactly_one(self, step: str, scope: str) -> tuple[StreamingFinding, ...]:
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=f"exactly_one {scope}",
                problem="exactly_one(...) computes input cardinality and is not compatible with streaming execution.",
                use="Keep this transform batch-only or enforce relation cardinality before the streaming transform.",
            ),
        )

    def _relation_assertion(self, step: str, operation: str) -> tuple[StreamingFinding, ...]:
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=operation,
                problem=f"{operation}(...) computes validation aggregates and is not compatible with streaming execution.",
                use="Keep this transform batch-only or enforce relation assertions before the streaming transform.",
            ),
        )

    def _relation_set(
        self,
        step: str,
        operation: str,
        input_name: str,
        input_source: str,
        *,
        allow_missing_columns: bool,
        current_streaming: bool,
        input_modes: dict[str, bool],
    ) -> tuple[StreamingFinding, ...]:
        input_streaming = bool(input_modes.get(input_source))
        if operation in {"union_all", "union_by_name"}:
            if allow_missing_columns:
                return (
                    StreamingFinding(
                        code="STREAM-E0801",
                        support=StreamingSupport.BATCH_ONLY,
                        step=step,
                        operation=f"{operation} {input_name}",
                        problem=(
                            f"{operation}(allow_missing_columns=True) is batch-only until live streaming "
                            "restart evidence proves nullable missing-column fills are safe."
                        ),
                        use="Use exact-schema stream-stream union, or materialize one side before missing-column union.",
                    ),
                )
            if current_streaming and input_streaming:
                return ()
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation=f"{operation} {input_name}",
                    problem=(
                        f"{operation}(...) is admitted for streaming only when both exact-schema relations are "
                        "declared with streaming=True."
                    ),
                    use=(
                        "Declare both union inputs with streaming=True, or materialize the static side before a "
                        "batch-only union boundary."
                    ),
                ),
            )
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=f"{operation} {input_name}",
                problem=f"{operation}(...) is not compatible with streaming relation-set execution.",
                use="Use union_all(...) or union_by_name(...) for stream-stream append composition, or keep this transform batch-only.",
            ),
        )

    def _priority_selection(self, step: str) -> tuple[StreamingFinding, ...]:
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation="select_first_qualified",
                problem=(
                    "select_first_qualified(...) uses ranking and validation aggregates and is streaming-ineligible "
                    "for caller-owned streaming execution."
                ),
                use="Keep this transform batch-only or perform priority selection before the streaming transform.",
            ),
        )

    def _hierarchy_closure(self, step: str) -> tuple[StreamingFinding, ...]:
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation="hierarchy_closure",
                problem="hierarchy_closure(...) expands bounded parent rows and is not compatible with streaming execution.",
                use="Keep this transform batch-only or materialize hierarchy closure before the streaming transform.",
            ),
        )

    def _hierarchy_fallbacks(self, step: str) -> tuple[StreamingFinding, ...]:
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation="hierarchy_fallbacks",
                problem="hierarchy_fallbacks(...) expands bounded parent fallback rows and is not compatible with streaming execution.",
                use="Keep this transform batch-only or materialize hierarchy fallbacks before the streaming transform.",
            ),
        )

    def _relation_ordering(self, step: str, operation: str) -> tuple[StreamingFinding, ...]:
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=operation,
                problem=(
                    f"{operation}(...) is streaming-ineligible for unbounded caller-owned Structured Streaming "
                    "relations."
                ),
                use="Keep this transform batch-only or move ordering and bounded selection to a batch materialization boundary.",
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
                    problem="drop_duplicates_within_watermark(...) requires an input declared with streaming=True.",
                    use="Declare the current input with streaming=True or use drop_duplicates(...) for cross-mode transforms.",
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
        self, step: str, expressions: tuple[PySparkExpressionRecipe, ...], *, streaming: bool
    ) -> tuple[StreamingFinding, ...]:
        if not streaming or not any(self._has_window(expression) for expression in expressions):
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
        input_modes: dict[str, bool],
        current_input: str,
        current_scope: str,
        watermarks: dict[str, set[str]],
    ) -> tuple[StreamingFinding, ...]:
        if not input_modes.get(current_input) and not input_modes.get(join.source):
            return ()
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
        if input_modes.get(join.source):
            return self._stream_stream_join(
                step,
                join,
                input_modes=input_modes,
                current_input=current_input,
                current_scope=current_scope,
                watermarks=watermarks,
            )
        if join.method is JoinMethod.NOT_EXISTS and input_modes.get(current_input):
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation=f"stream-static anti join {join.input_name}",
                    problem=(
                        "Stream-static anti joins are not admitted because the supported streaming shape defines only the left-semi "
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
                    "Streaming execution supports stream-static left and inner joins only; "
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
        input_modes: dict[str, bool],
        current_input: str,
        current_scope: str,
        watermarks: dict[str, set[str]],
    ) -> tuple[StreamingFinding, ...]:
        if join.how is Join.CROSS:
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation=f"stream-stream cross join {join.input_name}",
                    problem=(
                        "A stream-stream cross join has Cartesian state. Watermarks without an event-time pair "
                        "cannot establish finite retention for every possible row pair."
                    ),
                    use=(
                        "Materialize one side as a static snapshot, or keep this candidate caller-owned until a "
                        "versioned bounded cross-join contract proves retention and restart semantics."
                    ),
                ),
            )
        if join.method is JoinMethod.NOT_EXISTS:
            return (
                StreamingFinding(
                    code="STREAM-E0801",
                    support=StreamingSupport.BATCH_ONLY,
                    step=step,
                    operation=f"stream-stream anti join {join.input_name}",
                    problem=(
                        "A stream-stream anti join can revise an emitted left row when a late right-side match "
                        "arrives. Structure has no finite completion, timeout, and append-final contract for this "
                        "candidate."
                    ),
                    use=(
                        "Use a supported bounded stream-stream semi join, materialize the right side as a static "
                        "snapshot, or keep the anti join caller-owned until restart evidence proves its completion "
                        "policy."
                    ),
                ),
            )
        admitted = (
            join.method is JoinMethod.ROWSET and join.how in {Join.INNER, Join.LEFT, Join.RIGHT, Join.FULL}
        ) or join.method is JoinMethod.EXISTS
        if (
            admitted
            and input_modes.get(current_input)
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
                    f"Stream-stream {shape} joins require both inputs declared with streaming=True, watermarks "
                    "on both bound event-time fields, and an event_time_between(...) constraint."
                ),
                use=(
                    "Declare both inputs with streaming=True, call watermark(...) for both bound event-time fields, "
                    "and include event_time_between(left_time, right_time, upper=...) in the join predicate."
                ),
            ),
        )

    def _admitted_stream_stream_join(
        self,
        join: PySparkJoinRecipe,
        *,
        input_modes: dict[str, bool],
        current_input: str,
        current_scope: str,
        watermarks: dict[str, set[str]],
    ) -> bool:
        admitted = (
            join.method is JoinMethod.ROWSET and join.how in {Join.INNER, Join.LEFT, Join.RIGHT, Join.FULL}
        ) or join.method is JoinMethod.EXISTS
        return (
            admitted
            and bool(input_modes.get(join.source))
            and bool(input_modes.get(current_input))
            and self._watermarked_time_bound(join.predicate, current_scope, join.input_name, watermarks)
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
        if hook.streaming:
            return ()
        return (
            StreamingFinding(
                code="STREAM-W0801",
                support=StreamingSupport.UNKNOWN,
                step=step,
                operation=f"{hook.phase} hook {hook.name}",
                problem="Hooks are arbitrary PySpark code. Structure cannot prove this hook is streaming-compatible.",
                use=(
                    f"Mark {hook.name} with streaming=True only if it avoids Spark actions, "
                    "RDD/Pandas conversion, streaming lifecycle APIs, and stateful streaming operations. Keep "
                    "readStream, writeStream, checkpoints, triggers, output modes, query start/stop, and foreach "
                    "side effects in caller-owned PySpark code such as examples/streams/adoption.py."
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
