from __future__ import annotations

from structure.dsl import Transform
from structure.plugin.api.v1.model import RuntimeDiagnostic, StructureRuntimeError, TransformResult
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.compiler.model.PySparkWatermarkRecipe import PySparkWatermarkRecipe
from structure.plugin.pyspark.dsl.joins import JoinMethod
from structure.plugin.pyspark.dsl.types import ArrayType, StructType
from structure.plugin.pyspark.execution.logic.expressions.EvaluatePySparkExpression import EvaluatePySparkExpression
from structure.plugin.pyspark.execution.logic.InvokePySparkHooks import InvokePySparkHooks
from structure.plugin.pyspark.execution.logic.running.RunOnlinePySparkStructGenerator import (
    RunOnlinePySparkStructGenerator,
)
from structure.plugin.pyspark.execution.logic.ValidatePySparkFrame import ValidatePySparkFrame


class RunOnlinePySparkTransform:

    def __init__(self) -> None:
        self._expressions = EvaluatePySparkExpression()
        self._hooks = InvokePySparkHooks()
        self._struct_generators = RunOnlinePySparkStructGenerator()
        self._validator = ValidatePySparkFrame()
        self._backend_target = ">=3.5,<4.1"

    @property
    def _schema(self):
        from structure.plugin.pyspark.api.PySpark import PySpark

        return PySpark.schema

    def __call__(
        self,
        invocation: Transform,
        plan: PySparkExecutionPlan,
        *,
        session,
    ) -> TransformResult:
        if session.online_executor is not None:
            result = session.online_executor(
                plan=plan,
                inputs=invocation._structure_bound_inputs,
                spark=session.spark,
                ctx=session.ctx,
            )
            if isinstance(result, TransformResult):
                return result
            if len(plan.outputs) == 1:
                return TransformResult(
                    {plan.outputs[0].name: result},
                    single=True,
                    aliases=self._output_aliases(plan),
                )
            raise TypeError("Injected online executor must return TransformResult for multi-output transforms")
        if session.spark is None:
            raise self._missing_executor(invocation, session=session)

        return self._run(invocation, plan, session=session)

    def _run(self, invocation: Transform, plan: PySparkExecutionPlan, *, session):
        from pyspark.sql import Window  # type: ignore[import-not-found]
        from pyspark.sql import functions as F  # type: ignore[import-not-found]
        from pyspark.sql import types as T  # type: ignore[import-not-found]

        inputs = invocation._structure_bound_inputs
        self._backend_target = plan.backend.target
        for input in plan.inputs:
            self._validator.validate(inputs[input.name], input.validation, types=T)

        frames = dict(inputs)
        frames.update({f"input:{name}": frame for name, frame in inputs.items()})
        for step in plan.steps:
            produced = self._step(
                step,
                current=frames[step.source],
                frames=frames,
                inputs=inputs,
                invocation=invocation,
                session=session,
                functions=F,
                window=Window,
                types=T,
            )
            frames.update(produced)

        outputs = {}
        for output in plan.outputs:
            outputs[output.name] = self._output(
                output,
                source=frames[output.source],
                inputs=inputs,
                functions=F,
                window=Window,
                types=T,
            )
        return TransformResult(outputs, single=len(plan.outputs) == 1, aliases=self._output_aliases(plan))

    def _output_aliases(self, plan: PySparkExecutionPlan) -> dict[str, tuple[str, ...]]:
        return {output.name: output.aliases for output in plan.outputs if output.aliases}

    def _step(
        self,
        step: PySparkStepRecipe,
        *,
        current,
        frames,
        inputs,
        invocation: Transform,
        session,
        functions,
        window,
        types,
    ):
        active = current
        if step.before_hooks:
            self._hooks.apply(
                step.before_hooks,
                frames=frames,
                invocation=invocation,
                session=session,
            )
            active = frames[step.source]

        df = active.alias(step.input_alias)
        df = self._operations(step, df, frames=frames, functions=functions, window=window, types=types)

        if len(step.results) > 1:
            produced = {}
            for result in step.results:
                projected = df.select(
                    *(
                        self._assignment(assignment, step=step, functions=functions, window=window, types=types)
                        for assignment in result.projection
                    )
                )
                produced[result.frame] = projected
            for result in step.results:
                if result.after_hooks:
                    hook_frames = dict(frames)
                    hook_frames.update(produced)
                    self._hooks.apply(
                        result.after_hooks,
                        frames=hook_frames,
                        invocation=invocation,
                        session=session,
                    )
                    produced.update({name: hook_frames[name] for hook in result.after_hooks for name in hook.outputs})
                projected = produced[result.frame]
                for validation in result.validations:
                    if validation.check:
                        self._validator.validate(projected, validation, types=types)
                    if validation.project:
                        projected = self._validator.project(projected, validation, types=types, functions=functions)
                projected = self._post_operations(step, projected)
                produced[result.frame] = projected
            return produced

        if step.projection:
            df = df.select(
                *(
                    self._assignment(assignment, step=step, functions=functions, window=window, types=types)
                    for assignment in step.projection
                )
            )
        if step.after_hooks:
            step_frames = dict(frames)
            step_frames[step.results[0].frame] = df
            self._hooks.apply(
                step.after_hooks,
                frames=step_frames,
                invocation=invocation,
                session=session,
            )
            df = step_frames[step.results[0].frame]
        for validation in step.validations:
            if validation.check:
                self._validator.validate(df, validation, types=types)
            if validation.project:
                df = self._validator.project(df, validation, types=types, functions=functions)
        df = self._post_operations(step, df)
        return {step.results[0].frame: df}

    def _output(
        self,
        output: PySparkOutputRecipe,
        *,
        source,
        inputs,
        functions,
        window,
        types,
    ):
        df = source.alias(output.input_alias)
        df = self._operations(output, df, frames=inputs, functions=functions, window=window, types=types)

        if output.projection:
            df = df.select(
                *(
                    self._assignment(assignment, step=output, functions=functions, window=window, types=types)
                    for assignment in output.projection
                )
            )
        self._validator.validate(df, output.validation, types=types)
        df = self._post_operations(output, df)
        return df

    def _operations(self, step: PySparkStepRecipe | PySparkOutputRecipe, df, *, frames, functions, window, types):
        if not step.operations:
            for join in step.joins:
                df = self._join(
                    step,
                    df,
                    join,
                    frames=frames,
                    functions=functions,
                    window=window,
                    watermarks=(),
                )
            for filter in step.filters:
                df = df.where(
                    self._expressions.evaluate(
                        filter, functions=functions, aliases=self._scope_aliases(step), window=window
                    )
                )
            return df

        prepared_frames = dict(frames)
        joined_scopes: set[str] = set()
        for operation in step.operations:
            if operation.kind == "join" and operation.join is not None:
                df = self._join(
                    step,
                    df,
                    operation.join,
                    frames=prepared_frames,
                    functions=functions,
                    window=window,
                    watermarks=self._right_watermarks(step, operation.join),
                )
                joined_scopes.add(operation.join.input_name)
            if operation.kind == "filter" and operation.filter is not None:
                df = df.where(
                    self._expressions.evaluate(
                        operation.filter,
                        functions=functions,
                        aliases=self._scope_aliases(step),
                        window=window,
                    )
                )
            if operation.kind == "aggregate" and operation.aggregate is not None:
                df = self._aggregate(step, df, operation.aggregate, functions=functions, types=types)
            if operation.kind == "selected_rows" and operation.selected_rows is not None:
                df = self._selected_rows(step, df, operation.selected_rows, functions=functions, window=window)
            if operation.kind == "drop_duplicates":
                duplicate_rows = operation.duplicate_rows
                subset = () if duplicate_rows is None else duplicate_rows.subset
                scope = None if duplicate_rows is None else duplicate_rows.scope
                within_watermark = False if duplicate_rows is None else duplicate_rows.within_watermark
                if (
                    scope
                    and scope != getattr(step, "source_scope", None)
                    and scope not in joined_scopes
                    and any(join.input_name == scope for join in step.joins)
                ):
                    source = self._source_for_scope(step, scope)
                    frame = prepared_frames[source] if source in prepared_frames else prepared_frames[scope]
                    prepared = self._drop_duplicates(frame, subset, within_watermark=within_watermark)
                    prepared_frames[source] = prepared
                    prepared_frames[scope] = prepared
                else:
                    df = self._drop_duplicates(df, subset, within_watermark=within_watermark)
            if operation.kind == "exactly_one" and operation.exactly_one is not None:
                scope = operation.exactly_one.scope
                if scope == getattr(step, "source_scope", None):
                    df = self._exactly_one(df, scope, functions=functions)
                else:
                    source = self._source_for_scope(step, scope)
                    frame = prepared_frames[source] if source in prepared_frames else prepared_frames[scope]
                    prepared = self._exactly_one(frame, scope, functions=functions)
                    prepared_frames[source] = prepared
                    prepared_frames[scope] = prepared
            if operation.kind == "posexplode_struct" and operation.posexplode_struct is not None:
                df = self._posexplode_struct(step, df, operation.posexplode_struct, functions=functions, types=types)
            if operation.kind == "posexplode_outer_struct" and operation.posexplode_struct is not None:
                df = self._posexplode_struct(step, df, operation.posexplode_struct, functions=functions, types=types)
            if operation.kind == "explode_struct" and operation.posexplode_struct is not None:
                df = self._posexplode_struct(step, df, operation.posexplode_struct, functions=functions, types=types)
            if operation.kind == "explode_outer_struct" and operation.posexplode_struct is not None:
                df = self._posexplode_struct(step, df, operation.posexplode_struct, functions=functions, types=types)
            if operation.kind == "inline_struct" and operation.posexplode_struct is not None:
                df = self._posexplode_struct(step, df, operation.posexplode_struct, functions=functions, types=types)
            if operation.kind == "inline_outer_struct" and operation.posexplode_struct is not None:
                df = self._posexplode_struct(step, df, operation.posexplode_struct, functions=functions, types=types)
            if operation.kind == "ordered_timeline_scan" and operation.ordered_timeline_scan is not None:
                df = self._ordered_timeline_scan(
                    step,
                    df,
                    operation.ordered_timeline_scan,
                    functions=functions,
                    types=types,
                )
            if operation.relation_alias is not None:
                continue
            if operation.relation_assertion is not None:
                df = self._relation_assertion(
                    step,
                    df,
                    operation.relation_assertion,
                    frames=prepared_frames,
                    functions=functions,
                )
            if operation.relation_order is not None:
                df = self._relation_order(step, df, operation.relation_order, functions=functions)
            if operation.relation_bound is not None:
                df = getattr(df, operation.kind)(operation.relation_bound.count)
            if operation.relation_priority_selection is not None:
                df = self._relation_priority_selection(
                    step,
                    df,
                    operation.relation_priority_selection,
                    functions=functions,
                    window=window,
                )
            if operation.relation_hierarchy_closure is not None:
                df = self._relation_hierarchy_closure(
                    step,
                    df,
                    operation.relation_hierarchy_closure,
                    functions=functions,
                    types=types,
                )
            if operation.relation_hierarchy_fallback is not None:
                parent_source = operation.relation_hierarchy_fallback.parent_source
                parent_frame = (
                    prepared_frames[parent_source]
                    if parent_source in prepared_frames
                    else prepared_frames[operation.relation_hierarchy_fallback.parent_input]
                )
                df = self._relation_hierarchy_fallbacks(
                    step,
                    df,
                    operation.relation_hierarchy_fallback,
                    parent_frame=parent_frame,
                    functions=functions,
                    types=types,
                )
            if operation.relation_set is not None:
                source = operation.relation_set.source
                frame = (
                    prepared_frames[source]
                    if source in prepared_frames
                    else prepared_frames[operation.relation_set.input_name]
                )
                df = self._relation_set(df, frame, operation.relation_set)
            if operation.kind == "watermark" and operation.watermark is not None:
                if operation.watermark.scope == getattr(step, "source_scope", ""):
                    df = self._watermark(operation.watermark, df)
        return df

    def _watermark(self, watermark: PySparkWatermarkRecipe, frame):
        return frame.withWatermark(watermark.column, watermark.delay)

    def _right_watermarks(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        join: PySparkJoinRecipe,
    ) -> tuple[PySparkWatermarkRecipe, ...]:
        return tuple(
            operation.watermark
            for operation in step.operations
            if operation.kind == "watermark"
            and operation.watermark is not None
            and operation.watermark.scope == join.input_name
        )

    def _source_for_scope(self, step: PySparkStepRecipe | PySparkOutputRecipe, scope: str) -> str:
        for join in step.joins:
            if join.input_name == scope:
                return join.source
        return scope

    def _post_operations(self, step: PySparkStepRecipe | PySparkOutputRecipe, df):
        for operation in step.operations:
            if operation.kind == "cache":
                cache = operation.cache
                if cache is None or cache.storage_level is None:
                    df = df.persist()
                else:
                    from pyspark import StorageLevel  # type: ignore[import-not-found]

                    df = df.persist(StorageLevel(*cache.storage_level))
        return df

    def _drop_duplicates_subset(self, subset) -> list[str] | None:
        if not subset:
            return None
        return [self._field_column(expression) for expression in subset]

    def _drop_duplicates(self, frame, subset, *, within_watermark: bool):
        columns = self._drop_duplicates_subset(subset)
        if within_watermark or getattr(frame, "isStreaming", False):
            return frame.dropDuplicatesWithinWatermark(columns)
        return frame.dropDuplicates(columns)

    def _exactly_one(self, frame, scope: str, *, functions):
        message = f"REL-E0701: exactly_one({scope}) requires exactly one row; see docs/Diagnostics.md#rel-e0701"
        assertion = frame.agg(functions.count(functions.lit(1)).alias("__structure_count")).select(
            functions.assert_true(
                functions.col("__structure_count") == functions.lit(1),
                message,
            ).alias("__structure_exactly_one")
        )
        return assertion.crossJoin(frame).drop("__structure_exactly_one")

    def _relation_assertion(self, step, frame, assertion, *, frames, functions):
        if assertion.operation == "require_unique":
            return self._require_unique(step, frame, assertion, functions=functions)
        if assertion.operation == "require_all":
            return self._require_all(step, frame, assertion, functions=functions)
        if assertion.operation == "require_reference":
            return self._require_reference(step, frame, assertion, frames=frames, functions=functions)
        if assertion.operation == "require_parent_hierarchy":
            return self._require_parent_hierarchy(step, frame, assertion, functions=functions)
        raise TypeError(f"Unsupported relation assertion: {assertion.operation}")

    def _require_unique(self, step, frame, assertion, *, functions):
        message = "REL-E0702: require_unique(...) found duplicate keys; see docs/Diagnostics.md#rel-e0702"
        keys = tuple(
            self._expressions.evaluate(expression, functions=functions, aliases=self._scope_aliases(step))
            for expression in assertion.keys
        )
        duplicates = frame.groupBy(*keys).agg(functions.count(functions.lit(1)).alias("__structure_count"))
        violations = duplicates.where(functions.col("__structure_count") > functions.lit(1)).agg(
            functions.count(functions.lit(1)).alias("__structure_violations")
        )
        guard = violations.select(
            functions.assert_true(
                functions.col("__structure_violations") == functions.lit(0),
                message,
            ).alias("__structure_require_unique")
        )
        return guard.crossJoin(frame).drop("__structure_require_unique")

    def _require_all(self, step, frame, assertion, *, functions):
        assert assertion.predicate is not None
        message = (
            "REL-E0703: require_all(...) found rows that do not satisfy the predicate; "
            "see docs/Diagnostics.md#rel-e0703"
        )
        predicate = self._expressions.evaluate(assertion.predicate, functions=functions, aliases=self._scope_aliases(step))
        violations = frame.where(~functions.coalesce(predicate, functions.lit(False))).agg(
            functions.count(functions.lit(1)).alias("__structure_violations")
        )
        guard = violations.select(
            functions.assert_true(
                functions.col("__structure_violations") == functions.lit(0),
                message,
            ).alias("__structure_require_all")
        )
        return guard.crossJoin(frame).drop("__structure_require_all")

    def _require_reference(self, step, frame, assertion, *, frames, functions):
        assert assertion.value is not None
        assert assertion.reference_key is not None
        message = "REL-E0704: require_reference(...) found values without a reference row; see docs/Diagnostics.md#rel-e0704"
        value_column = "__structure_reference_value"
        key_column = "__structure_reference_key"
        reference = (
            frames[assertion.reference_source]
            if assertion.reference_source in frames
            else frames[assertion.reference_input]
        )
        value = self._expressions.evaluate(assertion.value, functions=functions, aliases=self._scope_aliases(step))
        reference_key = self._expressions.evaluate(
            assertion.reference_key,
            functions=functions,
            aliases={
                assertion.reference_input: "",
                assertion.reference_schema.__name__: "",
            },
        )
        left = frame.withColumn(value_column, value)
        right = reference.select(reference_key.alias(key_column)).dropDuplicates([key_column])
        candidates = left.where(functions.col(value_column).isNotNull()) if assertion.nulls == "allow" else left
        violations = candidates.join(
            right,
            functions.col(value_column) == functions.col(key_column),
            "left_anti",
        ).agg(functions.count(functions.lit(1)).alias("__structure_violations"))
        guard = violations.select(
            functions.assert_true(
                functions.col("__structure_violations") == functions.lit(0),
                message,
            ).alias("__structure_require_reference")
        )
        return guard.crossJoin(frame).drop("__structure_require_reference")

    def _require_parent_hierarchy(self, step, frame, assertion, *, functions):
        assert assertion.parent is not None
        assert assertion.order_by is not None
        assert assertion.max_depth is not None
        aliases = self._scope_aliases(step)
        node = "__structure_hierarchy_node"
        parent = "__structure_hierarchy_parent"
        order = "__structure_hierarchy_order"
        path = "__structure_hierarchy_path"
        message = (
            "REL-E0706: require_parent_hierarchy(...) found missing parent, cycle, depth overrun, "
            "or non-increasing child order; see docs/Diagnostics.md#rel-e0706"
        )
        node_id = self._expressions.evaluate(assertion.keys[0], functions=functions, aliases=aliases)
        parent_id = self._expressions.evaluate(assertion.parent, functions=functions, aliases=aliases)
        order_by = self._expressions.evaluate(self._order_value(assertion.order_by), functions=functions, aliases=aliases)
        nodes = frame.select(node_id.alias(node), parent_id.alias(parent), order_by.alias(order))
        known = nodes.select(functions.col(node).alias("__structure_hierarchy_known_parent"))
        missing = nodes.where(functions.col(parent).isNotNull()).join(
            known,
            functions.col(parent) == functions.col("__structure_hierarchy_known_parent"),
            "left_anti",
        )
        parent_order = nodes.alias("child").join(
            nodes.alias("parent"),
            functions.col(f"child.{parent}") == functions.col(f"parent.{node}"),
            "inner",
        )
        parent_order = parent_order.where(~(functions.col(f"child.{order}") > functions.col(f"parent.{order}")))
        parent_order = parent_order.select(
            functions.col(f"child.{node}").alias(node),
            functions.col(f"child.{parent}").alias(parent),
            functions.col(f"child.{order}").alias(order),
        )
        frontier = nodes.where(functions.col(parent).isNotNull()).select(
            functions.col(node),
            functions.col(parent),
            functions.col(order),
            functions.array(functions.col(node)).alias(path),
        )
        cycles = frontier.where(functions.array_contains(functions.col(path), functions.col(parent)))
        for _ in range(assertion.max_depth):
            frontier = frontier.where(
                functions.col(parent).isNotNull()
                & ~functions.array_contains(functions.col(path), functions.col(parent))
            )
            frontier = frontier.withColumn(path, functions.array_append(functions.col(path), functions.col(parent)))
            frontier = frontier.alias("frontier").join(
                nodes.alias("next_parent"),
                functions.col(f"frontier.{parent}") == functions.col(f"next_parent.{node}"),
                "left",
            ).select(
                functions.col(f"frontier.{node}").alias(node),
                functions.col(f"next_parent.{parent}").alias(parent),
                functions.col(f"frontier.{order}").alias(order),
                functions.col(f"frontier.{path}").alias(path),
            )
            cycles = cycles.unionByName(
                frontier.where(functions.array_contains(functions.col(path), functions.col(parent))),
                allowMissingColumns=False,
            )
        overrun = frontier.where(functions.col(parent).isNotNull())
        violations = missing.unionByName(
            parent_order,
            allowMissingColumns=False,
        ).unionByName(
            cycles.select(missing.columns),
            allowMissingColumns=False,
        ).unionByName(
            overrun.select(missing.columns),
            allowMissingColumns=False,
        )
        violations = violations.agg(functions.count(functions.lit(1)).alias("__structure_violations"))
        guard = violations.select(
            functions.assert_true(
                functions.col("__structure_violations") == functions.lit(0),
                message,
            ).alias("__structure_require_parent_hierarchy")
        )
        return guard.crossJoin(frame).drop("__structure_require_parent_hierarchy")

    def _posexplode_struct(self, step, frame, generator, *, functions, types):
        aliases = self._scope_aliases(step)
        value = self._expressions.evaluate(generator.expression, functions=functions, aliases=aliases)
        return self._struct_generators(frame, generator, functions=functions, types=types, value=value)

    def _ordered_timeline_scan(self, step, frame, scan, *, functions, types):
        prefix = f"__structure_{scan.scope.strip('_')}"
        partition_columns = tuple(f"{prefix}_partition_{index}" for index, _ in enumerate(scan.partition_by))
        order_columns = tuple(f"{prefix}_order_{index}" for index, _ in enumerate(scan.order_by))
        items = f"{prefix}_items"
        folded = f"{prefix}_folded"
        row = f"{prefix}_row"
        position = f"{prefix}_pos"
        guard_column = f"__structure_{scan.scope.strip('_')}_guard"
        payload = "__payload"
        state = "__state"
        rows = "__rows"
        input_columns = tuple(frame.columns)
        aliases = self._scope_aliases(step)

        keyed = frame
        for column, expression in zip(partition_columns, scan.partition_by, strict=True):
            keyed = keyed.withColumn(column, self._expressions.evaluate(expression, functions=functions, aliases=aliases))
        for column, expression in zip(order_columns, scan.order_by, strict=True):
            keyed = keyed.withColumn(column, self._expressions.evaluate(expression, functions=functions, aliases=aliases))

        guard = self._scan_guard(keyed, partition_columns, order_columns, scan, functions=functions)
        keyed = guard.crossJoin(keyed)

        item = functions.struct(
            *(functions.col(column).alias(column) for column in order_columns),
            functions.struct(*(functions.col(column).alias(column) for column in input_columns)).alias(payload),
        )
        grouped = keyed.groupBy(*partition_columns, guard_column).agg(
            functions.sort_array(functions.collect_list(item)).alias(items)
        )

        state_schema = self._scan_state_schema(scan.state_schema, types=types)
        initial_state = functions.struct(
            *(
                self._expressions.evaluate(expression, functions=functions, aliases=aliases)
                .cast(state_schema[scan.state_schema._structure_fields[name].column].dataType)
                .alias(scan.state_schema._structure_fields[name].column)
                for name, expression in scan.initial
            )
        )
        initial_accumulator = functions.struct(
            initial_state.alias(state),
            functions.array().cast(self._scan_rows_type(frame, input_columns, scan, types=types)).alias(rows),
        )

        def merge(accumulator, item):
            current_state = accumulator.getField(state)
            current_rows = accumulator.getField(rows)
            before = functions.struct(item.getField(payload).alias(payload), current_state.alias(state))
            next_state = functions.struct(
                *(
                    self._scan_transition_value(expression, scan, accumulator, item, functions=functions)
                    .cast(state_schema[scan.state_schema._structure_fields[name].column].dataType)
                    .alias(scan.state_schema._structure_fields[name].column)
                    for name, expression in scan.transition
                )
            )
            return functions.struct(
                next_state.alias(state),
                functions.concat(current_rows, functions.array(before)).alias(rows),
            )

        folded_frame = grouped.select(functions.aggregate(functions.col(items), initial_accumulator, merge).alias(folded))
        expanded = folded_frame.select(functions.posexplode(functions.col(folded).getField(rows)).alias(position, row))
        return expanded.select(
            *(functions.col(f"{row}.{payload}.{column}").alias(column) for column in input_columns),
            *(
                functions.col(f"{row}.{state}.{field.column}").alias(field.column)
                for field in scan.state_schema._structure_fields.values()
            ),
        )

    def _scan_guard(self, keyed, partition_columns, order_columns, scan, *, functions):
        violation = "__structure_scan_violation"
        count = "__structure_scan_count"
        null_order = None
        for column in order_columns:
            condition = functions.col(column).isNull()
            null_order = condition if null_order is None else null_order | condition
        assert null_order is not None
        nulls = keyed.where(null_order).select(functions.lit(1).alias(violation))
        duplicates = (
            keyed.groupBy(*partition_columns, *order_columns)
            .agg(functions.count(functions.lit(1)).alias(count))
            .where(functions.col(count) > functions.lit(1))
            .select(functions.lit(1).alias(violation))
        )
        overruns = (
            keyed.groupBy(*partition_columns)
            .agg(functions.count(functions.lit(1)).alias(count))
            .where(functions.col(count) > functions.lit(scan.max_rows))
            .select(functions.lit(1).alias(violation))
        )
        message = (
            "SCAN-E0801: scan(...) found null order keys, duplicate order keys, or a partition above max_rows; "
            "see docs/dev/specifications/OrderedTimelineScan.md"
        )
        violations = nulls.unionByName(duplicates, allowMissingColumns=False).unionByName(
            overruns,
            allowMissingColumns=False,
        )
        return violations.agg(functions.count(functions.lit(1)).alias(count)).select(
            functions.assert_true(functions.col(count) == functions.lit(0), message).alias(
                f"__structure_{scan.scope.strip('_')}_guard"
            )
        )

    def _scan_rows_type(self, frame, input_columns, scan, *, types):
        payload_type = types.StructType([frame.schema[column] for column in input_columns])
        state_type = self._scan_state_schema(scan.state_schema, types=types)
        row_type = types.StructType(
            [
                types.StructField("__payload", payload_type, nullable=False),
                types.StructField("__state", state_type, nullable=False),
            ]
        )
        return types.ArrayType(row_type, containsNull=False)

    def _scan_state_schema(self, schema, *, types):
        return types.StructType(
            [
                types.StructField(field.column, self._scan_state_type(field.type, types=types), field.nullable)
                for field in schema._structure_fields.values()
            ]
        )

    def _scan_state_type(self, type_, *, types):
        from structure.plugin.pyspark.dsl.types import ArrayType, MapType, StructType

        if isinstance(type_, ArrayType):
            return types.ArrayType(self._scan_state_type(type_.element, types=types), containsNull=True)
        if isinstance(type_, MapType):
            return types.MapType(
                self._scan_state_type(type_.key, types=types),
                self._scan_state_type(type_.value, types=types),
                valueContainsNull=True,
            )
        if isinstance(type_, StructType):
            return self._scan_state_schema(type_.schema, types=types)
        return self._schema.materialize().type(type_, types=types)

    def _scan_transition_value(self, expression, scan, accumulator, item, *, functions):
        state = accumulator.getField("__state")
        payload = item.getField("__payload")
        rewritten = self._scan_rewrite(expression, scan, state=state, payload=payload)
        return self._expressions.evaluate(rewritten, functions=functions, aliases={})

    def _scan_rewrite(self, expression, scan, *, state, payload):
        if expression.kind == "field" and "scope" in expression.data:
            scope = str(expression.data["scope"])
            if scope == scan.state_scope:
                return self._scan_field(expression, state)
            if scope == scan.row_scope:
                return self._scan_field(expression, payload)
        return PySparkExpressionRecipe(
            kind=expression.kind,
            type=expression.type,
            nullable=expression.nullable,
            data=expression.data,
            args=tuple(self._scan_rewrite(argument, scan, state=state, payload=payload) for argument in expression.args),
        )

    def _scan_field(self, expression, root):
        rewritten = PySparkExpressionRecipe(
            kind="lambda_arg",
            type=None,
            nullable=False,
            data={"column": root},
        )
        path = expression.data.get("path", (expression.data["field"],))
        for field in path:
            rewritten = PySparkExpressionRecipe(
                kind="get_field",
                type=expression.type,
                nullable=expression.nullable,
                data={"field": str(field)},
                args=(rewritten,),
            )
        return rewritten

    def _relation_set(self, left, right, relation_set):
        if relation_set.by_name:
            return left.unionByName(right, allowMissingColumns=False)
        function = {
            "union_all": "union",
            "intersect": "intersect",
            "intersect_all": "intersectAll",
            "subtract": "subtract",
            "except_all": "exceptAll",
        }[relation_set.operation]
        return getattr(left, function)(right)

    def _relation_order(self, step, frame, relation_order, *, functions):
        return frame.orderBy(
            *(
                self._expressions.evaluate(expression, functions=functions, aliases=self._scope_aliases(step))
                for expression in relation_order.order_by
            )
        )

    def _relation_priority_selection(self, step, frame, selection, *, functions, window):
        aliases = self._scope_aliases(step)
        key_columns = tuple(f"__structure_priority_key_{position}" for position, _ in enumerate(selection.keys))
        priority = "__structure_priority_order"
        rank = "__structure_priority_rank"
        keys = tuple(
            self._expressions.evaluate(expression, functions=functions, aliases=aliases)
            for expression in selection.keys
        )
        predicate = self._expressions.evaluate(selection.predicate, functions=functions, aliases=aliases)
        priority_value = self._expressions.evaluate(
            self._order_value(selection.order_by), functions=functions, aliases=aliases
        )
        ordering = self._expressions.evaluate(selection.order_by, functions=functions, aliases=aliases)

        all_keys = frame.select(*(key.alias(column) for key, column in zip(keys, key_columns, strict=True)))
        all_keys = all_keys.dropDuplicates(list(key_columns))
        eligible = frame.where(functions.coalesce(predicate, functions.lit(False))).withColumn(priority, priority_value)
        eligible_keys = eligible.select(*(key.alias(column) for key, column in zip(keys, key_columns, strict=True)))
        eligible_keys = eligible_keys.dropDuplicates(list(key_columns))

        guards = []
        if selection.missing == "error":
            message = (
                "REL-E0705: select_first_qualified(...) found a key without an eligible candidate; "
                "see docs/Diagnostics.md#rel-e0705"
            )
            missing = all_keys.join(eligible_keys, list(key_columns), "left_anti")
            missing = missing.agg(functions.count(functions.lit(1)).alias("__structure_violations"))
            guards.append(
                missing.select(
                    functions.assert_true(
                        functions.col("__structure_violations") == functions.lit(0),
                        message,
                    ).alias("__structure_select_first_missing")
                )
            )

        message = (
            "REL-E0705: select_first_qualified(...) found tied eligible candidates; "
            "see docs/Diagnostics.md#rel-e0705"
        )
        ties = eligible.select(
            *(key.alias(column) for key, column in zip(keys, key_columns, strict=True)),
            functions.col(priority),
        )
        ties = ties.groupBy(*key_columns, priority).agg(functions.count(functions.lit(1)).alias("__structure_count"))
        ties = ties.where(functions.col("__structure_count") > functions.lit(1))
        ties = ties.agg(functions.count(functions.lit(1)).alias("__structure_violations"))
        guards.append(
            ties.select(
                functions.assert_true(
                    functions.col("__structure_violations") == functions.lit(0),
                    message,
                ).alias("__structure_select_first_ties")
            )
        )

        ranked = eligible.withColumn(
            rank,
            functions.row_number().over(window.partitionBy(*keys).orderBy(ordering)),
        )
        ranked = ranked.where(functions.col(rank) == functions.lit(1))
        guarded = guards[0]
        for guard in guards[1:]:
            guarded = guarded.crossJoin(guard)
        return guarded.crossJoin(ranked).drop(
            rank,
            priority,
            "__structure_select_first_missing",
            "__structure_select_first_ties",
        )

    def _order_value(self, expression):
        return expression.args[0] if expression.kind == "order" else expression

    def _relation_hierarchy_closure(self, step, frame, closure, *, functions, types):
        aliases = self._scope_aliases(step)
        source_node = "__structure_hierarchy_node"
        source_parent = "__structure_hierarchy_parent"
        node = closure.schema._structure_fields[closure.node].column
        ancestor = closure.schema._structure_fields[closure.ancestor].column
        depth = closure.schema._structure_fields[closure.depth].column
        id_expression = self._expressions.evaluate(closure.id, functions=functions, aliases=aliases)
        parent_expression = self._expressions.evaluate(closure.parent, functions=functions, aliases=aliases)
        nodes = frame.select(id_expression.alias(source_node), parent_expression.alias(source_parent))
        closure_frame = nodes.select(
            functions.col(source_node).alias(node),
            functions.col(source_node).alias(ancestor),
            functions.lit(0).cast(types.LongType()).alias(depth),
        )
        frontier = nodes
        for depth_value in range(1, closure.max_depth + 1):
            branch = frontier.where(functions.col(source_parent).isNotNull()).select(
                functions.col(source_node).alias(node),
                functions.col(source_parent).alias(ancestor),
                functions.lit(depth_value).cast(types.LongType()).alias(depth),
            )
            closure_frame = closure_frame.unionByName(branch, allowMissingColumns=False)
            frontier = frontier.where(functions.col(source_parent).isNotNull())
            frontier = frontier.alias("frontier").join(
                nodes.alias("parent"),
                functions.col(f"frontier.{source_parent}") == functions.col(f"parent.{source_node}"),
                "left",
            ).select(
                functions.col(f"frontier.{source_node}").alias(source_node),
                functions.col(f"parent.{source_parent}").alias(source_parent),
            )
        return closure_frame

    def _relation_hierarchy_fallbacks(self, step, frame, fallback, *, parent_frame, functions, types):
        aliases = self._scope_aliases(step)
        parent_aliases = {
            fallback.parent_input: "",
            fallback.parent_schema.__name__: "",
        }
        source_key = "__structure_fallback_source"
        path = "__structure_fallback_path"
        parent_node = "__structure_fallback_parent_node"
        parent_value = "__structure_fallback_parent_value"
        last = "__structure_fallback_last"
        source = fallback.schema._structure_fields[fallback.source].column
        fallback_column = fallback.schema._structure_fields[fallback.fallback].column
        ordinal = fallback.schema._structure_fields[fallback.ordinal].column
        source_id = self._expressions.evaluate(fallback.source_id, functions=functions, aliases=aliases)
        path_expression = self._expressions.evaluate(fallback.path, functions=functions, aliases=aliases)
        parent_id = self._expressions.evaluate(fallback.parent_id, functions=functions, aliases=parent_aliases)
        parent = self._expressions.evaluate(fallback.parent, functions=functions, aliases=parent_aliases)
        parents = parent_frame.select(parent_id.alias(parent_node), parent.alias(parent_value))
        frontier = frame.select(source_id.alias(source_key), path_expression.alias(path))
        result = frontier.select(
            functions.col(source_key).alias(source),
            self._online_fallback_id(functions, fallback, path).alias(fallback_column),
            functions.lit(0).cast(types.LongType()).alias(ordinal),
        )
        for ordinal_value in range(1, fallback.max_depth + 1):
            active = frontier.where(functions.size(functions.col(path)) > functions.lit(0))
            joined = active.withColumn(last, functions.element_at(functions.col(path), functions.lit(-1))).join(
                parents,
                functions.col(last) == functions.col(parent_node),
                "left",
            )
            frontier = joined.select(
                functions.col(source_key).alias(source_key),
                self._online_fallback_next_path(functions, path, parent_value).alias(path),
            )
            branch = frontier.select(
                functions.col(source_key).alias(source),
                self._online_fallback_id(functions, fallback, path).alias(fallback_column),
                functions.lit(ordinal_value).cast(types.LongType()).alias(ordinal),
            )
            result = result.unionByName(branch, allowMissingColumns=False)
        return result

    def _online_fallback_next_path(self, functions, path: str, parent: str):
        head = functions.slice(
            functions.col(path),
            functions.lit(1),
            functions.size(functions.col(path)) - functions.lit(1),
        )
        return functions.when(functions.col(parent).isNull(), head).when(
            functions.array_contains(head, functions.col(parent)),
            head,
        ).otherwise(functions.concat(head, functions.array(functions.col(parent))))

    def _online_fallback_id(self, functions, fallback, path: str):
        return functions.when(
            functions.size(functions.col(path)) > functions.lit(0),
            functions.sha2(functions.concat_ws(fallback.separator, functions.col(path)), 256),
        )

    def _field_column(self, expression) -> str:
        if expression.kind != "field":
            raise TypeError("drop_duplicates(...) subset can only use field expressions")
        return str(expression.data["field"])

    def _selected_rows(self, step, df, selected_rows, *, functions, window):
        rank = f"__structure_{step.name}_{selected_rows.direction}_rank"
        aliases = self._scope_aliases(step)
        partition = tuple(
            self._expressions.evaluate(expression, functions=functions, aliases=aliases)
            for expression in selected_rows.partition_by
        )
        order_by = self._expressions.evaluate(selected_rows.order_by, functions=functions, aliases=aliases)
        ordering = order_by.desc() if selected_rows.direction == "latest" else order_by.asc()
        ranked = df.withColumn(rank, functions.row_number().over(window.partitionBy(*partition).orderBy(ordering)))
        return ranked.where(functions.col(rank) == functions.lit(1)).drop(rank)

    def _aggregate(self, step, df, aggregate, *, functions, types):
        if aggregate.grouping == "grouping_sets":
            return self._grouping_sets(step, df, aggregate, functions=functions, types=types)
        if aggregate.grouping == "group_by":
            group = df.groupBy
        elif aggregate.grouping == "rollup":
            group = df.rollup
        elif aggregate.grouping == "cube":
            group = df.cube
        else:
            raise TypeError(f"Unsupported aggregate grouping: {aggregate.grouping}")
        key_columns = self._aggregate_key_columns(aggregate) if aggregate.grouping in {"rollup", "cube"} else ()
        if aggregate.grouping in {"rollup", "cube"}:
            for key, column in key_columns:
                df = df.withColumn(
                    column,
                    self._expressions.evaluate(
                        key.expression,
                        functions=functions,
                        aliases=self._scope_aliases(step),
                    ),
                )
            group = df.rollup if aggregate.grouping == "rollup" else df.cube
        grouped = (
            df
            if aggregate.grouping == "group_by" and not aggregate.keys
            else group(
                *(
                    (
                        self._aggregate_key_column(key, key_columns)
                        if aggregate.grouping in {"rollup", "cube"}
                        else self._expressions.evaluate(
                            key.expression,
                            functions=functions,
                            aliases=self._scope_aliases(step),
                        ).alias(key.name)
                    )
                    for key in aggregate.keys
                )
            )
        )
        aggregated = grouped.agg(
            *(
                self._aggregate_assignment(
                    assignment,
                    step=step,
                    aggregate=aggregate,
                    key_columns=key_columns,
                    functions=functions,
                    types=types,
                )
                for assignment in aggregate.assignments
                if assignment.function != "key"
            )
        )
        selected = aggregated.select(
            *(
                self._aggregate_select(
                    assignment,
                    key_columns=key_columns,
                    functions=functions,
                )
                for assignment in aggregate.assignments
            )
        )
        return self._aggregate_having(step, selected, aggregate, functions=functions)

    def _grouping_sets(self, step, df, aggregate, *, functions, types):
        key_columns = self._aggregate_key_columns(aggregate)
        for key, column in key_columns:
            df = df.withColumn(
                column,
                self._expressions.evaluate(
                    key.expression,
                    functions=functions,
                    aliases=self._scope_aliases(step),
                ),
            )
        branches = []
        for level in aggregate.levels:
            level_keys = set(level)
            grouped = df.groupBy(
                *(
                    functions.col(self._aggregate_key_column(key, key_columns))
                    for key in aggregate.keys
                    if key.name in level_keys
                )
            )
            aggregated = grouped.agg(
                *(
                    self._aggregate_assignment(
                        assignment,
                        step=step,
                        aggregate=aggregate,
                        key_columns=key_columns,
                        functions=functions,
                        types=types,
                    )
                    for assignment in aggregate.assignments
                    if assignment.function not in {"key", "grouping_id", "is_grouped"}
                )
            )
            branches.append(
                aggregated.select(
                    *(
                        self._grouping_set_select(
                            assignment,
                            aggregate=aggregate,
                            level=level_keys,
                            key_columns=key_columns,
                            functions=functions,
                            types=types,
                        )
                        for assignment in aggregate.assignments
                    )
                )
            )
        if not branches:
            raise TypeError("grouping_sets(...) requires at least one grouping level")
        result = branches[0]
        for branch in branches[1:]:
            result = result.unionByName(branch)
        return self._aggregate_having(step, result, aggregate, functions=functions)

    def _aggregate_having(self, step, df, aggregate, *, functions):
        if aggregate.having is None:
            return df
        aliases = {**self._scope_aliases(step), step.output_schema.__name__: ""}
        predicate = self._expressions.evaluate(aggregate.having, functions=functions, aliases=aliases)
        return df.where(predicate)

    def _grouping_set_select(self, assignment, *, aggregate, level, key_columns, functions, types):
        if assignment.function == "key":
            if assignment.key in level:
                return functions.col(self._grouping_set_key_column(assignment, key_columns=key_columns)).alias(
                    assignment.field.column
                )
            return (
                functions.lit(None).cast(self._spark_type(assignment.field.type, types)).alias(assignment.field.column)
            )
        if assignment.function == "grouping_id":
            return (
                functions.lit(self._grouping_id(aggregate, level=level))
                .cast(self._spark_type(assignment.field.type, types))
                .alias(assignment.field.column)
            )
        if assignment.function == "is_grouped":
            key = self._grouping_set_expression_key(assignment, aggregate=aggregate)
            return (
                functions.lit(key not in level)
                .cast(self._spark_type(assignment.field.type, types))
                .alias(assignment.field.column)
            )
        return functions.col(assignment.field.column)

    def _grouping_id(self, aggregate, *, level) -> int:
        mask = 0
        key_count = len(aggregate.keys)
        for index, key in enumerate(aggregate.keys):
            if key.name not in level:
                mask |= 1 << (key_count - index - 1)
        return mask

    def _grouping_set_expression_key(self, assignment, *, aggregate) -> str:
        if assignment.expression is None:
            raise TypeError("is_grouped(...) requires a grouping expression")
        for key in aggregate.keys:
            if assignment.expression == key.expression:
                return key.name
        raise TypeError("is_grouped(...) expression must match a grouping_sets(...) key")

    def _grouping_set_key_column(self, assignment, *, key_columns) -> str:
        for key, column in key_columns:
            if key.name == assignment.key:
                return column
        raise TypeError(f"Missing aggregate key column for {assignment.key}")

    def _aggregate_assignment(self, assignment, *, step, aggregate, key_columns, functions, types):
        if assignment.function == "count":
            column = functions.lit(1)
            if assignment.filter is not None:
                predicate = self._expressions.evaluate(
                    assignment.filter,
                    functions=functions,
                    aliases=self._scope_aliases(step),
                )
                column = functions.when(predicate, functions.lit(1))
            return (
                functions.count(column)
                .cast(self._spark_type(assignment.field.type, types))
                .alias(assignment.field.column)
            )
        if assignment.function == "grouping_id":
            return (
                functions.grouping_id()
                .cast(self._spark_type(assignment.field.type, types))
                .alias(assignment.field.column)
            )
        if assignment.function == "is_grouped" and assignment.expression is not None:
            column = self._aggregate_grouping_column(
                assignment,
                step=step,
                aggregate=aggregate,
                key_columns=key_columns,
                functions=functions,
            )
            return (
                functions.grouping(column)
                .cast(self._spark_type(assignment.field.type, types))
                .alias(assignment.field.column)
            )
        if (
            assignment.function == "collect_list"
            and assignment.order_by is not None
            and assignment.expression is not None
        ):
            return self._ordered_collect_list(assignment, step=step, functions=functions)
        if assignment.function == "mode" and assignment.expression is not None:
            column = self._expressions.evaluate(
                assignment.expression,
                functions=functions,
                aliases=self._scope_aliases(step),
            )
            if assignment.filter is not None:
                predicate = self._expressions.evaluate(
                    assignment.filter,
                    functions=functions,
                    aliases=self._scope_aliases(step),
                )
                column = functions.when(predicate, column)
            options = dict(assignment.options)
            if options.get("deterministic") is True:
                result = self._deterministic_mode(column, functions=functions)
            else:
                result = functions.mode(column)
            return result.cast(self._spark_type(assignment.field.type, types)).alias(assignment.field.column)
        arguments = assignment.arguments or (() if assignment.expression is None else (assignment.expression,))
        if assignment.function in self._aggregate_functions() and arguments:
            columns = [
                self._expressions.evaluate(argument, functions=functions, aliases=self._scope_aliases(step))
                for argument in arguments
            ]
            if assignment.filter is not None:
                predicate = self._expressions.evaluate(
                    assignment.filter,
                    functions=functions,
                    aliases=self._scope_aliases(step),
                )
                columns[0] = functions.when(predicate, columns[0])
            options = dict(assignment.options)
            if assignment.function == "approx_count_distinct" and "relative_sd" in options:
                columns.append(options["relative_sd"])
            if assignment.function == "approx_percentile":
                columns.append(options["percentage"])
                if "accuracy" in options:
                    columns.append(options["accuracy"])
            if assignment.function == "percentile":
                columns.extend((options["percentage"], options["frequency"]))
            result = self._aggregate_function(functions, assignment.function)(*columns)
            if not self._keeps_struct_collection_type(assignment):
                result = result.cast(self._spark_type(assignment.field.type, types))
            return result.alias(assignment.field.column)
        if assignment.function in {"first_value", "last_value"} and assignment.expression is not None:
            if assignment.order_by is None:
                raise TypeError(f"{assignment.function}(...) requires order_by")
            column = self._expressions.evaluate(
                assignment.expression, functions=functions, aliases=self._scope_aliases(step)
            )
            order_by = self._expressions.evaluate(
                assignment.order_by, functions=functions, aliases=self._scope_aliases(step)
            )
            if assignment.filter is not None:
                predicate = self._expressions.evaluate(
                    assignment.filter,
                    functions=functions,
                    aliases=self._scope_aliases(step),
                )
                order_by = functions.when(predicate, order_by)
            function = functions.min_by if assignment.function == "first_value" else functions.max_by
            return function(column, order_by).alias(assignment.field.column)
        if assignment.function == "first" and assignment.expression is not None:
            column = self._expressions.evaluate(
                assignment.expression,
                functions=functions,
                aliases=self._scope_aliases(step),
            )
            return functions.first(column, ignorenulls=False).alias(assignment.field.column)
        raise TypeError(f"Unsupported aggregate assignment: {assignment.function}")

    def _ordered_collect_list(self, assignment, *, step, functions):
        order = assignment.order_by
        assert order is not None and assignment.expression is not None
        descending = order.kind == "order" and order.data.get("direction") == "desc"
        key = order.args[0] if order.kind == "order" else order
        value = self._expressions.evaluate(
            assignment.expression,
            functions=functions,
            aliases=self._scope_aliases(step),
        )
        condition = value.isNotNull()
        if assignment.filter is not None:
            predicate = self._expressions.evaluate(
                assignment.filter,
                functions=functions,
                aliases=self._scope_aliases(step),
            )
            condition = predicate & condition
        order_column = self._expressions.evaluate(key, functions=functions, aliases=self._scope_aliases(step))
        item = functions.struct(order_column.alias("_structure_order"), value.alias("_structure_value"))
        collected = functions.collect_list(functions.when(condition, item))
        return functions.transform(
            functions.sort_array(collected, asc=not descending),
            lambda item: item.getField("_structure_value"),
        ).alias(assignment.field.column)

    def _keeps_struct_collection_type(self, assignment) -> bool:
        return (
            self._backend_target == ">=3.5,<4.0"
            and assignment.function in {"collect_list", "collect_set"}
            and isinstance(assignment.field.type, ArrayType)
            and isinstance(assignment.field.type.element, StructType)
        )

    def _deterministic_mode(self, column, *, functions):
        collected = functions.collect_list(column)
        counts = functions.transform(
            functions.array_distinct(collected),
            lambda candidate: functions.struct(
                candidate.alias("_structure_value"),
                functions.size(functions.filter(collected, lambda item: item == candidate)).alias("_structure_count"),
            ),
        )
        max_count = functions.array_max(functions.transform(counts, lambda item: item.getField("_structure_count")))
        tied = functions.transform(
            functions.filter(counts, lambda item: item.getField("_structure_count") == max_count),
            lambda item: item.getField("_structure_value"),
        )
        return functions.array_min(tied)

    def _aggregate_grouping_column(self, assignment, *, step, aggregate, key_columns, functions):
        if assignment.expression is None:
            raise TypeError("is_grouped(...) requires a grouping expression")
        for key in aggregate.keys:
            if assignment.expression == key.expression:
                return self._aggregate_key_column(key, key_columns)
        return self._expressions.evaluate(
            assignment.expression,
            functions=functions,
            aliases=self._scope_aliases(step),
        )

    def _aggregate_key_columns(self, aggregate):
        return tuple((key, f"__structure_group_{index}_{key.name}") for index, key in enumerate(aggregate.keys))

    def _aggregate_key_column(self, target, key_columns):
        for key, column in key_columns:
            if key == target:
                return column
        raise TypeError(f"Missing aggregate key column for {target.name}")

    def _aggregate_functions(self) -> set[str]:
        return {
            "approx_count_distinct",
            "approx_percentile",
            "avg",
            "bool_and",
            "bool_or",
            "collect_list",
            "collect_set",
            "corr",
            "covar",
            "count_distinct",
            "max",
            "mode",
            "min",
            "kurtosis",
            "percentile",
            "skewness",
            "stddev",
            "sum",
            "variance",
        }

    def _aggregate_function(self, functions, function: str):
        name = {
            "approx_count_distinct": "approx_count_distinct",
            "approx_percentile": "percentile_approx",
            "avg": "avg",
            "bool_and": "bool_and",
            "bool_or": "bool_or",
            "collect_list": "collect_list",
            "collect_set": "collect_set",
            "corr": "corr",
            "covar": "covar_samp",
            "count_distinct": "countDistinct",
            "max": "max",
            "mode": "mode",
            "min": "min",
            "kurtosis": "kurtosis",
            "percentile": "percentile",
            "skewness": "skewness",
            "stddev": "stddev",
            "sum": "sum",
            "variance": "variance",
        }[function]
        return getattr(functions, name)

    def _aggregate_select(self, assignment, *, key_columns, functions):
        source = self._aggregate_select_source(assignment, key_columns=key_columns)
        column = functions.col(str(source))
        if str(source) != assignment.field.column:
            return column.alias(assignment.field.column)
        return column

    def _aggregate_select_source(self, assignment, *, key_columns):
        if assignment.function == "key":
            for key, column in key_columns:
                if key.name == assignment.key:
                    return column
            if assignment.key is not None:
                return assignment.key
        return assignment.field.column

    def _spark_type(self, type, types):
        if types is None:
            return type.name
        return self._schema.materialize().type(type, types=types)

    def _join(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        df,
        join,
        *,
        frames,
        functions,
        window,
        watermarks: tuple[PySparkWatermarkRecipe, ...],
    ):
        row_id = None
        if join.as_of is not None:
            row_id = f"__structure_{join.left_alias}_{join.right_alias}_row"
            df = df.withColumn(row_id, functions.monotonically_increasing_id())
        right = frames[join.source]
        for watermark in watermarks:
            right = self._watermark(watermark, right)
        if join.strategy is not None:
            right = right.hint(join.strategy.hint())
        right = right.alias(join.right_alias)
        if join.dedupe is not None:
            right = self._dedupe(join, right, functions=functions, window=window)
        if join.hint is not None and join.hint.value == "broadcast":
            right = functions.broadcast(right)
        if join.how.value == "cross":
            joined = df.crossJoin(right)
        else:
            predicate = self._predicate(step, join, functions=functions)
            joined = df.join(right, predicate, self._join_mode(join))
        if join.as_of is not None:
            return self._as_of(join, joined, row_id=row_id, functions=functions, window=window)
        return joined

    def _predicate(self, step, join, *, functions):
        aliases = self._scope_aliases(step, join)
        predicate = self._expressions.evaluate(join.predicate, functions=functions, aliases=aliases)
        if join.temporal is not None:
            at = self._expressions.evaluate(join.temporal.at, functions=functions, aliases=aliases)
            valid_from = self._expressions.evaluate(join.temporal.valid_from, functions=functions, aliases=aliases)
            valid_to = self._expressions.evaluate(join.temporal.valid_to, functions=functions, aliases=aliases)
            predicate = predicate & (valid_from <= at) & ((at < valid_to) | valid_to.isNull())
        if join.as_of is not None:
            left_time = self._expressions.evaluate(join.as_of.left_time, functions=functions, aliases=aliases)
            right_time = self._expressions.evaluate(join.as_of.right_time, functions=functions, aliases=aliases)
            as_of = right_time <= left_time if join.as_of.direction.value == "backward" else right_time >= left_time
            if join.as_of.tolerance is not None:
                tolerance = self._expressions.evaluate(join.as_of.tolerance, functions=functions, aliases=aliases)
                bound = right_time >= left_time - tolerance
                if join.as_of.direction.value == "forward":
                    bound = right_time <= left_time + tolerance
                as_of = as_of & bound
            predicate = predicate & as_of
        return predicate

    def _as_of(self, join, df, *, row_id, functions, window):
        rank = f"__structure_{join.right_alias}_as_of_rank"
        right_time = self._expressions.evaluate(
            join.as_of.right_time,
            functions=functions,
            aliases={join.input_name: join.right_alias},
        )
        ranked = df.withColumn(
            rank,
            functions.row_number().over(
                window.partitionBy(functions.col(row_id)).orderBy(
                    right_time.desc() if join.as_of.direction.value == "backward" else right_time.asc()
                )
            ),
        )
        return ranked.where(functions.col(rank) == functions.lit(1)).drop(rank).drop(row_id)

    def _dedupe(self, join, right, *, functions, window):
        rank = f"__structure_{join.right_alias}_rank"
        partition = [
            self._expressions.evaluate(key, functions=functions, aliases={join.input_name: join.right_alias})
            for key in self._right_keys(join)
        ]
        order_by = self._expressions.evaluate(
            join.dedupe.order_by,
            functions=functions,
            aliases={join.input_name: join.right_alias},
        )
        ordering = order_by.desc() if join.dedupe.direction == "latest" else order_by.asc()
        ranked = right.withColumn(rank, functions.row_number().over(window.partitionBy(*partition).orderBy(ordering)))
        return ranked.where(functions.col(rank) == functions.lit(1)).drop(rank).alias(join.right_alias)

    def _right_keys(self, join: PySparkJoinRecipe):
        return tuple(self._right_key(join, condition) for condition in self._join_conditions(join.predicate))

    def _right_key(self, join: PySparkJoinRecipe, condition):
        left, right = condition.args
        if join.input_name in self._scopes(left):
            return left
        return right

    def _join_conditions(self, expression):
        if expression.kind == "and":
            return tuple(condition for argument in expression.args for condition in self._join_conditions(argument))
        if expression.kind in {"eq", "null_safe_eq"}:
            return (expression,)
        return ()

    def _scopes(self, expression) -> set[str]:
        scopes = set().union(*(self._scopes(argument) for argument in expression.args))
        if expression.kind == "field" and "scope" in expression.data:
            scopes.add(str(expression.data["scope"]))
        return scopes

    def _join_mode(self, join: PySparkJoinRecipe) -> str:
        if join.method is JoinMethod.EXISTS:
            return "left_semi"
        if join.method is JoinMethod.NOT_EXISTS:
            return "left_anti"
        return join.how.value

    def _assignment(self, assignment, *, step, functions, window, types):
        column = self._expressions.evaluate(
            assignment.expression,
            functions=functions,
            aliases=self._scope_aliases(step),
            window=window,
        )
        column = self._validator.cast(column, assignment.field, assignment.expression, types=types)
        return self._validator.alias(column, assignment.field, assignment.expression)

    def _scope_aliases(
        self, step: PySparkStepRecipe | PySparkOutputRecipe, join: PySparkJoinRecipe | None = None
    ) -> dict[str, str]:
        aliases = {
            step.input_schema.__name__: step.input_alias,
        }
        source_scope = getattr(step, "source_scope", None)
        if source_scope is not None:
            aliases[source_scope] = step.input_alias
        if step.ordinal == 0:
            aliases["orders"] = step.input_alias
        if any(operation.relation_set is not None for operation in step.operations):
            if source_scope is not None:
                aliases[source_scope] = ""
            aliases[step.input_schema.__name__] = ""
        for item in step.joins:
            aliases[item.input_name] = item.right_alias
        for operation in step.operations:
            if operation.posexplode_struct is not None:
                aliases[operation.posexplode_struct.scope] = ""
            if operation.ordered_timeline_scan is not None:
                aliases[operation.ordered_timeline_scan.row_scope] = ""
                aliases[operation.ordered_timeline_scan.scope] = ""
                aliases[step.input_schema.__name__] = ""
                source_scope = getattr(step, "source_scope", None)
                if source_scope is not None:
                    aliases[source_scope] = ""
            if operation.relation_hierarchy_closure is not None:
                aliases[operation.relation_hierarchy_closure.scope] = ""
            if operation.relation_hierarchy_fallback is not None:
                aliases[operation.relation_hierarchy_fallback.scope] = ""
        if join is not None:
            aliases[join.input_name] = join.right_alias
        return aliases

    def _missing_executor(self, invocation: Transform, *, session) -> StructureRuntimeError:
        transform = f"{type(invocation).__module__}.{type(invocation).__name__}"
        options = getattr(session, "plugin_options", {})
        diagnostic = RuntimeDiagnostic(
            code="ONLINE-E1202",
            title="Direct PySpark runner is not configured",
            transform=transform,
            execution_mode=session.execution_mode,
            target=getattr(session, "target", "pyspark"),
            problem="Structure has no live SparkSession or injected direct executor for this session.",
            use=(
                "Pass spark or online_executor to StructureSession, or switch to generated-code execution "
                'with execution_mode = "generated".'
            ),
            docs="docs/background/Execution.back.md",
            context={
                "target_profile": str(options.get("profile", ">=3.5,<4.1")),
                "target_variant": str(options.get("variant", "ordinary")),
            },
        )
        return StructureRuntimeError(diagnostic)


run_online_pyspark_transform = RunOnlinePySparkTransform()
