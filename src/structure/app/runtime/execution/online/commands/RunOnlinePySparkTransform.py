from __future__ import annotations

from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.runtime.execution.online.logic.PySparkExpressionEvaluator import PySparkExpressionEvaluator
from structure.app.runtime.execution.online.logic.PySparkFrameValidator import PySparkFrameValidator
from structure.app.runtime.execution.online.logic.PySparkHookInvoker import HookInputs, PySparkHookInvoker
from structure.app.runtime.session.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.app.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.app.runtime.session.model.TransformResult import TransformResult
from structure.app.target.pyspark.commands.MaterializePySparkSchema import materialize_pyspark_schema
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.app.target.pyspark.model.PySparkStepRecipe import PySparkStepRecipe


class RunOnlinePySparkTransform:

    def __init__(self) -> None:
        self._expressions = PySparkExpressionEvaluator()
        self._hooks = PySparkHookInvoker()
        self._validator = PySparkFrameValidator()

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
        for input in plan.inputs:
            self._validator.validate(inputs[input.name], input.validation, types=T)

        hook_inputs = HookInputs(**inputs) if plan.requires_hook_inputs else None
        frames = dict(inputs)
        frames.update({f"input:{name}": frame for name, frame in inputs.items()})
        for step in plan.steps:
            produced = self._step(
                step,
                current=frames[step.source],
                frames=frames,
                inputs=inputs,
                hook_inputs=hook_inputs,
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
        hook_inputs,
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
                inputs=hook_inputs,
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
                        inputs=hook_inputs,
                        invocation=invocation,
                        session=session,
                    )
                    produced.update({name: hook_frames[name] for hook in result.after_hooks for name in hook.outputs})
                projected = produced[result.frame]
                for validation in result.validations:
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
                inputs=hook_inputs,
                invocation=invocation,
                session=session,
            )
            df = step_frames[step.results[0].frame]
        for validation in step.validations:
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
                df = self._join(step, df, join, frames=frames, functions=functions, window=window)
            for filter in step.filters:
                df = df.where(
                    self._expressions.evaluate(filter, functions=functions, aliases=self._scope_aliases(step), window=window)
                )
            return df

        for operation in step.operations:
            if operation.kind == "join" and operation.join is not None:
                df = self._join(step, df, operation.join, frames=frames, functions=functions, window=window)
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
                subset = () if operation.duplicate_rows is None else operation.duplicate_rows.subset
                df = df.dropDuplicates(self._drop_duplicates_subset(subset))
        return df

    def _post_operations(self, step: PySparkStepRecipe | PySparkOutputRecipe, df):
        for operation in step.operations:
            if operation.kind == "cache":
                df = df.persist()
        return df

    def _drop_duplicates_subset(self, subset) -> list[str] | None:
        if not subset:
            return None
        return [self._field_column(expression) for expression in subset]

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
        if aggregate.grouping == "group_by":
            group = df.groupBy
        elif aggregate.grouping == "rollup":
            group = df.rollup
        elif aggregate.grouping == "cube":
            group = df.cube
        else:
            raise TypeError(f"Unsupported aggregate grouping: {aggregate.grouping}")
        key_columns = (
            self._aggregate_key_columns(aggregate)
            if aggregate.grouping in {"rollup", "cube"}
            else ()
        )
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
        grouped = group(
            *(
                self._aggregate_key_column(key, key_columns)
                if aggregate.grouping in {"rollup", "cube"}
                else self._expressions.evaluate(
                    key.expression,
                    functions=functions,
                    aliases=self._scope_aliases(step),
                ).alias(key.name)
                for key in aggregate.keys
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
        return aggregated.select(
            *(
                self._aggregate_select(
                    assignment,
                    key_columns=key_columns,
                    functions=functions,
                )
                for assignment in aggregate.assignments
            )
        )

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
            return functions.grouping_id().cast(self._spark_type(assignment.field.type, types)).alias(assignment.field.column)
        if assignment.function == "is_grouped" and assignment.expression is not None:
            column = self._aggregate_grouping_column(
                assignment,
                step=step,
                aggregate=aggregate,
                key_columns=key_columns,
                functions=functions,
            )
            return functions.grouping(column).cast(self._spark_type(assignment.field.type, types)).alias(assignment.field.column)
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
            return (
                self._aggregate_function(functions, assignment.function)(*columns)
                .cast(self._spark_type(assignment.field.type, types))
                .alias(assignment.field.column)
            )
        if assignment.function in {"first_value", "last_value"} and assignment.expression is not None:
            if assignment.order_by is None:
                raise TypeError(f"{assignment.function}(...) requires order_by")
            column = self._expressions.evaluate(assignment.expression, functions=functions, aliases=self._scope_aliases(step))
            order_by = self._expressions.evaluate(assignment.order_by, functions=functions, aliases=self._scope_aliases(step))
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
        return tuple(
            (key, f"__structure_group_{index}_{key.name}")
            for index, key in enumerate(aggregate.keys)
        )

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
            "min",
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
            "min": "min",
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
        return materialize_pyspark_schema.type(type, types=types)

    def _join(self, step: PySparkStepRecipe | PySparkOutputRecipe, df, join, *, frames, functions, window):
        row_id = None
        if join.as_of is not None:
            row_id = f"__structure_{join.left_alias}_{join.right_alias}_row"
            df = df.withColumn(row_id, functions.monotonically_increasing_id())
        right = frames[join.source]
        if join.strategy is not None:
            right = right.hint(join.strategy.value)
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
            as_of = right_time <= left_time
            if join.as_of.tolerance is not None:
                tolerance = self._expressions.evaluate(join.as_of.tolerance, functions=functions, aliases=aliases)
                as_of = as_of & (right_time >= left_time - tolerance)
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
            functions.row_number().over(window.partitionBy(functions.col(row_id)).orderBy(right_time.desc())),
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
        for item in step.joins:
            aliases[item.input_name] = item.right_alias
        if join is not None:
            aliases[join.input_name] = join.right_alias
        return aliases

    def _missing_executor(self, invocation: Transform, *, session) -> StructureRuntimeError:
        transform = f"{type(invocation).__module__}.{type(invocation).__name__}"
        diagnostic = RuntimeDiagnostic(
            code="ONLINE-E1202",
            title="Online PySpark runner is not configured",
            transform=transform,
            execution_mode=session.execution_mode,
            target_backend=session.target_backend,
            target_profile=getattr(session, "target_profile", ">=3.5,<4.1"),
            target_variant=getattr(session, "target_variant", "ordinary"),
            problem="Structure has no live SparkSession or injected online executor for this session.",
            use="Pass spark or online_executor to StructureSession, or use execution_mode = \"generated\".",
            docs="docs/reference/OnlineExecution.md",
        )
        return StructureRuntimeError(diagnostic)


run_online_pyspark_transform = RunOnlinePySparkTransform()
