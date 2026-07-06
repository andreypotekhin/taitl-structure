from __future__ import annotations

import json
from typing import cast

from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.dsl.model.transforms.Join import Join
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.StructType import StructType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.target.pyspark.commands.RenderPySparkExpression import render_pyspark_expression
from structure.app.target.pyspark.commands.RenderPySparkSchema import render_pyspark_schema
from structure.app.target.pyspark.model.PySparkAggregateAssignment import PySparkAggregateAssignment
from structure.app.target.pyspark.model.PySparkAggregateKey import PySparkAggregateKey
from structure.app.target.pyspark.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.app.target.pyspark.model.PySparkDuplicateRowsRecipe import PySparkDuplicateRowsRecipe
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.app.target.pyspark.model.PySparkSelectedRowsRecipe import PySparkSelectedRowsRecipe
from structure.app.target.pyspark.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


class RenderPySparkStep:

    def __call__(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        *,
        current: str,
        sources: dict[str, str] | None = None,
    ) -> str:
        if isinstance(step, PySparkStepRecipe) and len(step.results) > 1:
            return self._multiple(step, current=current, sources=sources or {})
        target = self._target(step)
        lines = [f"        # Subtransform: {step.name}"]
        active = current
        if step.before_hooks:
            lines.extend(self._hooks(step.before_hooks))
        lines.append(f'        {target} = {active}.alias("{step.input_alias}")')
        lines.extend(self._operations(step, sources=sources or {}, target=target))
        lines.extend(self._projection(step, target=target))
        lines.extend(self._hooks(step.after_hooks))
        lines.extend(self._validations(step.validations, target=target))
        lines.extend(self._post_operations(step, target=target))
        return "\n".join(lines)

    def _multiple(self, step: PySparkStepRecipe, *, current: str, sources: dict[str, str]) -> str:
        lines = [f"        # Subtransform: {step.name}"]
        active = current
        if step.before_hooks:
            lines.extend(self._hooks(step.before_hooks))
        base = f"{step.name}_base"
        lines.append(f'        {base} = {active}.alias("{step.input_alias}")')
        lines.extend(self._operations(step, sources=sources, target=base))
        for result in step.results:
            lines.extend(self._result_projection(step, result, base=base))
        for result in step.results:
            lines.extend(self._hooks(result.after_hooks))
            lines.extend(self._validations(result.validations, target=result.frame))
            lines.extend(self._post_operations(step, target=result.frame))
        return "\n".join(lines)

    def _result_projection(self, step: PySparkStepRecipe, result, *, base: str) -> list[str]:
        lines = [f"        {result.frame} = {base}.select("]
        for assignment in result.projection:
            lines.append(f"            {self._assignment(assignment, scope_aliases=self._scope_aliases(step))},")
        lines.append("        )")
        return lines

    def _hooks(
        self,
        hooks: tuple[PySparkHookRecipe, ...],
    ) -> list[str]:
        lines: list[str] = []
        for hook in hooks:
            inputs = ", inputs=inputs" if hook.pass_inputs else ""
            arguments = ", ".join(f"{lane}={lane}" for lane in hook.lanes)
            if inputs:
                arguments = f"{arguments}{inputs}"
            outputs = ", ".join(hook.outputs)
            lines.append(f"        {outputs} = self._impl.{hook.name}({arguments}, spark=self.spark, ctx=self.ctx)")
        return lines

    def _joins(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        *,
        sources: dict[str, str],
        target: str = "df",
    ) -> list[str]:
        lines: list[str] = []
        for join in step.joins:
            lines.extend(self._join(step, join, sources=sources, target=target))
        return lines

    def _operations(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        *,
        sources: dict[str, str],
        target: str,
    ) -> list[str]:
        if not step.operations:
            lines = self._joins(step, sources=sources, target=target)
            if step.filters:
                lines.extend(self._filters(step.filters, step=step, target=target))
            return lines

        ordered_lines: list[str] = []
        pending_filters: list[PySparkExpressionRecipe] = []
        for operation in step.operations:
            if operation.kind == "filter" and operation.filter is not None:
                pending_filters.append(operation.filter)
                continue
            if pending_filters:
                ordered_lines.extend(self._filters(tuple(pending_filters), step=step, target=target))
                pending_filters = []
            if operation.kind == "join" and operation.join is not None:
                ordered_lines.extend(self._join(step, operation.join, sources=sources, target=target))
            if operation.kind == "aggregate" and operation.aggregate is not None:
                ordered_lines.extend(self._aggregate(step, operation.aggregate, target=target))
            if operation.kind == "selected_rows" and operation.selected_rows is not None:
                ordered_lines.extend(self._selected_rows(step, operation.selected_rows, target=target))
            if operation.kind == "drop_duplicates":
                duplicate_rows = operation.duplicate_rows or PySparkDuplicateRowsRecipe()
                ordered_lines.append(f"        {target} = {target}.dropDuplicates({self._dedupe_subset(duplicate_rows)})")
        if pending_filters:
            ordered_lines.extend(self._filters(tuple(pending_filters), step=step, target=target))
        return ordered_lines

    def _post_operations(self, step: PySparkStepRecipe | PySparkOutputRecipe, *, target: str) -> list[str]:
        return [f"        {target} = {target}.persist()" for operation in step.operations if operation.kind == "cache"]

    def _dedupe_subset(self, duplicate_rows: PySparkDuplicateRowsRecipe) -> str:
        if not duplicate_rows.subset:
            return ""
        return json.dumps(tuple(self._field_column(expression) for expression in duplicate_rows.subset))

    def _field_column(self, expression: PySparkExpressionRecipe) -> str:
        if expression.kind != "field":
            raise TypeError("drop_duplicates(...) subset can only render field expressions")
        return str(expression.data["field"])

    def _selected_rows(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        selected_rows: PySparkSelectedRowsRecipe,
        *,
        target: str,
    ) -> list[str]:
        rank = f"__structure_{step.name}_{selected_rows.direction}_rank"
        aliases = self._scope_aliases(step)
        partition = ", ".join(
            render_pyspark_expression(expression, scope_aliases=aliases) for expression in selected_rows.partition_by
        )
        order_by = render_pyspark_expression(selected_rows.order_by, scope_aliases=aliases)
        ordering = f"{order_by}.desc()" if selected_rows.direction == "latest" else f"{order_by}.asc()"
        window = f"Window.partitionBy({partition}).orderBy({ordering})"
        return [
            f'        {target} = {target}.withColumn("{rank}", F.row_number().over({window}))',
            f'        {target} = {target}.where(F.col("{rank}") == F.lit(1))',
            f'        {target} = {target}.drop("{rank}")',
        ]

    def _aggregate(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        aggregate: PySparkAggregateRecipe,
        *,
        target: str,
    ) -> list[str]:
        grouping = {"group_by": "groupBy", "rollup": "rollup", "cube": "cube"}.get(aggregate.grouping)
        if grouping is None:
            raise TypeError(f"Unsupported aggregate grouping: {aggregate.grouping}")
        key_columns = (
            self._aggregate_key_columns(aggregate)
            if aggregate.grouping in {"rollup", "cube"}
            else ()
        )
        lines = []
        if aggregate.grouping in {"rollup", "cube"}:
            for key, column in key_columns:
                expression = render_pyspark_expression(key.expression, scope_aliases=self._scope_aliases(step))
                lines.append(f"        {target} = {target}.withColumn({self._literal(column)}, {expression})")
        lines.append(f"        {target} = {target}.{grouping}(")
        for key in aggregate.keys:
            if aggregate.grouping in {"rollup", "cube"}:
                lines.append(f"            {self._literal(self._aggregate_key_column(key, key_columns))},")
            else:
                expression = render_pyspark_expression(key.expression, scope_aliases=self._scope_aliases(step))
                lines.append(f"            {expression}.alias({self._literal(key.name)}),")
        lines.append("        ).agg(")
        for assignment in aggregate.assignments:
            if assignment.function == "key":
                continue
            lines.append(
                f"            {self._aggregate_assignment(assignment, step=step, aggregate=aggregate, key_columns=key_columns)},"
            )
        lines.append("        ).select(")
        for assignment in aggregate.assignments:
            lines.append(f"            {self._aggregate_select(assignment, key_columns=key_columns)},")
        lines.append("        )")
        return lines

    def _aggregate_assignment(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        step,
        aggregate: PySparkAggregateRecipe,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        alias = self._literal(assignment.field.column)
        if assignment.function == "count":
            expression = "F.lit(1)"
            if assignment.filter is not None:
                predicate = render_pyspark_expression(assignment.filter, scope_aliases=self._scope_aliases(step))
                expression = f"F.when({predicate}, F.lit(1))"
            return f"F.count({expression}).cast({render_pyspark_schema.type(assignment.field.type)}).alias({alias})"
        if assignment.function == "grouping_id":
            return f"F.grouping_id().cast({render_pyspark_schema.type(assignment.field.type)}).alias({alias})"
        if assignment.function == "is_grouped" and assignment.expression is not None:
            expression = self._aggregate_grouping_expression(
                assignment,
                step=step,
                aggregate=aggregate,
                key_columns=key_columns,
            )
            return f"F.grouping({expression}).cast({render_pyspark_schema.type(assignment.field.type)}).alias({alias})"
        arguments = assignment.arguments or (() if assignment.expression is None else (assignment.expression,))
        if assignment.function in self._aggregate_functions() and arguments:
            rendered_arguments = [
                render_pyspark_expression(argument, scope_aliases=self._scope_aliases(step))
                for argument in arguments
            ]
            if assignment.filter is not None:
                predicate = render_pyspark_expression(assignment.filter, scope_aliases=self._scope_aliases(step))
                rendered_arguments[0] = f"F.when({predicate}, {rendered_arguments[0]})"
            function = self._aggregate_function(assignment.function)
            options = dict(assignment.options)
            if assignment.function == "approx_count_distinct" and "relative_sd" in options:
                rendered_arguments.append(repr(options["relative_sd"]))
            if assignment.function == "approx_percentile":
                rendered_arguments.append(repr(options["percentage"]))
                if "accuracy" in options:
                    rendered_arguments.append(repr(options["accuracy"]))
            return (
                f"{function}({', '.join(rendered_arguments)}).cast({render_pyspark_schema.type(assignment.field.type)})"
                f".alias({alias})"
            )
        if assignment.function in {"first_value", "last_value"} and assignment.expression is not None:
            if assignment.order_by is None:
                raise TypeError(f"{assignment.function}(...) requires order_by")
            value = render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))
            order_by = render_pyspark_expression(assignment.order_by, scope_aliases=self._scope_aliases(step))
            function = "F.min_by" if assignment.function == "first_value" else "F.max_by"
            return f"{function}({value}, {order_by}).alias({alias})"
        if assignment.function == "first" and assignment.expression is not None:
            expression = render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))
            return f"F.first({expression}, ignorenulls=False).alias({alias})"
        raise TypeError(f"Unsupported aggregate assignment: {assignment.function}")

    def _aggregate_grouping_expression(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        step,
        aggregate: PySparkAggregateRecipe,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        if assignment.expression is None:
            raise TypeError("is_grouped(...) requires a grouping expression")
        for key in aggregate.keys:
            if assignment.expression == key.expression:
                return self._literal(self._aggregate_key_column(key, key_columns))
        return render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))

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

    def _aggregate_function(self, function: str) -> str:
        return {
            "approx_count_distinct": "F.approx_count_distinct",
            "approx_percentile": "F.percentile_approx",
            "avg": "F.avg",
            "bool_and": "F.bool_and",
            "bool_or": "F.bool_or",
            "collect_list": "F.collect_list",
            "collect_set": "F.collect_set",
            "corr": "F.corr",
            "covar": "F.covar_samp",
            "count_distinct": "F.countDistinct",
            "max": "F.max",
            "min": "F.min",
            "stddev": "F.stddev",
            "sum": "F.sum",
            "variance": "F.variance",
        }[function]

    def _aggregate_select(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        source = self._aggregate_select_source(assignment, key_columns=key_columns)
        expression = f"F.col({self._literal(str(source))})"
        if str(source) != assignment.field.column:
            expression = f"{expression}.alias({self._literal(assignment.field.column)})"
        return expression

    def _aggregate_select_source(
        self,
        assignment: PySparkAggregateAssignment,
        *,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        if assignment.function == "key":
            for key, column in key_columns:
                if key.name == assignment.key:
                    return column
            if assignment.key is not None:
                return assignment.key
        return assignment.field.column

    def _aggregate_key_columns(
        self,
        aggregate: PySparkAggregateRecipe,
    ) -> tuple[tuple[PySparkAggregateKey, str], ...]:
        return tuple(
            (key, f"__structure_group_{index}_{key.name}")
            for index, key in enumerate(aggregate.keys)
        )

    def _aggregate_key_column(
        self,
        target: PySparkAggregateKey,
        key_columns: tuple[tuple[PySparkAggregateKey, str], ...],
    ) -> str:
        for key, column in key_columns:
            if key == target:
                return column
        raise TypeError(f"Missing aggregate key column for {target.name}")

    def _join(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        join: PySparkJoinRecipe,
        *,
        sources: dict[str, str],
        target: str,
    ) -> list[str]:
        source = sources.get(join.source, join.source)
        right = source
        if join.strategy is not None:
            right = f'{right}.hint("{join.strategy.value}")'
        right = f'{right}.alias("{join.right_alias}")'
        if join.dedupe is not None:
            right = self._dedupe(join, right=right)
        if join.hint is not None and join.hint.value == "broadcast":
            right = f"F.broadcast({right})"
        predicate = self._predicate(step, join)
        right_name = f"{join.right_alias}_joined"
        lines = []
        row_id = None
        if join.as_of is not None:
            row_id = f"__structure_{join.left_alias}_{join.right_alias}_row"
            lines.append(f'        {target} = {target}.withColumn("{row_id}", F.monotonically_increasing_id())')
        lines.append(f"        {right_name} = {right}")
        if join.how is Join.CROSS:
            lines.append(f"        {target} = {target}.crossJoin({right_name})")
        else:
            lines.extend(
                [
                    f"        {target} = {target}.join(",
                    f"            {right_name},",
                    f"            {predicate},",
                    f'            "{self._join_mode(join)}",',
                    "        )",
                ]
            )
        if join.as_of is not None:
            lines.extend(self._as_of(join, target=target, row_id=cast(str, row_id)))
        return lines

    def _predicate(self, step: PySparkStepRecipe | PySparkOutputRecipe, join: PySparkJoinRecipe) -> str:
        aliases = self._scope_aliases(step, join)
        predicate = render_pyspark_expression(join.predicate, scope_aliases=aliases)
        if join.temporal is None and join.as_of is None:
            return predicate
        if join.temporal is not None:
            at = render_pyspark_expression(join.temporal.at, scope_aliases=aliases)
            valid_from = render_pyspark_expression(join.temporal.valid_from, scope_aliases=aliases)
            valid_to = render_pyspark_expression(join.temporal.valid_to, scope_aliases=aliases)
            valid_window = f"(({valid_from} <= {at}) & (({at} < {valid_to}) | {valid_to}.isNull()))"
            predicate = f"({predicate} & {valid_window})"
        if join.as_of is not None:
            left_time = render_pyspark_expression(join.as_of.left_time, scope_aliases=aliases)
            right_time = render_pyspark_expression(join.as_of.right_time, scope_aliases=aliases)
            as_of = f"({right_time} <= {left_time})"
            if join.as_of.tolerance is not None:
                tolerance = render_pyspark_expression(join.as_of.tolerance, scope_aliases=aliases)
                as_of = f"({as_of} & ({right_time} >= ({left_time} - {tolerance})))"
            predicate = f"({predicate} & {as_of})"
        return predicate

    def _as_of(self, join: PySparkJoinRecipe, *, target: str, row_id: str) -> list[str]:
        as_of = join.as_of
        if as_of is None:
            raise TypeError("Cannot render as-of lookup without an as-of recipe")
        rank = f"__structure_{join.right_alias}_as_of_rank"
        right_time = render_pyspark_expression(as_of.right_time, scope_aliases={join.input_name: join.right_alias})
        window = f'Window.partitionBy(F.col("{row_id}")).orderBy({right_time}.desc())'
        return [
            f'        {target} = {target}.withColumn("{rank}", F.row_number().over({window}))',
            f'        {target} = {target}.where(F.col("{rank}") == F.lit(1))',
            f'        {target} = {target}.drop("{rank}").drop("{row_id}")',
        ]

    def _dedupe(self, join: PySparkJoinRecipe, *, right: str) -> str:
        dedupe = join.dedupe
        if dedupe is None:
            raise TypeError("Cannot render lookup dedupe without a dedupe recipe")
        rank = f"__structure_{join.right_alias}_rank"
        partition = ", ".join(
            render_pyspark_expression(key, scope_aliases={join.input_name: join.right_alias})
            for key in self._right_keys(join)
        )
        order_by = render_pyspark_expression(dedupe.order_by, scope_aliases={join.input_name: join.right_alias})
        ordering = f"{order_by}.desc()" if dedupe.direction == "latest" else f"{order_by}.asc()"
        window = f"Window.partitionBy({partition}).orderBy({ordering})"
        return (
            f'{right}.withColumn("{rank}", F.row_number().over({window}))'
            f'.where(F.col("{rank}") == F.lit(1))'
            f'.drop("{rank}")'
            f'.alias("{join.right_alias}")'
        )

    def _right_keys(self, join: PySparkJoinRecipe) -> tuple[PySparkExpressionRecipe, ...]:
        return tuple(self._right_key(join, condition) for condition in self._join_conditions(join.predicate))

    def _right_key(self, join: PySparkJoinRecipe, condition: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
        left, right = condition.args
        if join.input_name in self._scopes(left):
            return left
        return right

    def _join_conditions(self, expression: PySparkExpressionRecipe) -> tuple[PySparkExpressionRecipe, ...]:
        if expression.kind == "and":
            return tuple(condition for argument in expression.args for condition in self._join_conditions(argument))
        if expression.kind in {"eq", "null_safe_eq"}:
            return (expression,)
        return ()

    def _scopes(self, expression: PySparkExpressionRecipe) -> set[str]:
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

    def _filters(
        self,
        filters: tuple[PySparkExpressionRecipe, ...],
        *,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        target: str = "df",
    ) -> list[str]:
        predicate = " & ".join(
            f"({render_pyspark_expression(filter, scope_aliases=self._scope_aliases(step))})" for filter in filters
        )
        return [f"        {target} = {target}.where({predicate})"]

    def _projection(self, step: PySparkStepRecipe | PySparkOutputRecipe, *, target: str) -> list[str]:
        if not step.projection:
            return []
        lines = [f"        {target} = {target}.select("]
        for assignment in step.projection:
            lines.append(f"            {self._assignment(assignment, scope_aliases=self._scope_aliases(step))},")
        lines.append("        )")
        return lines

    def _assignment(self, assignment, *, scope_aliases: dict[str, str]) -> str:
        expression = render_pyspark_expression(assignment.expression, scope_aliases=scope_aliases)
        if self._needs_cast(assignment):
            expression = f"{expression}.cast({render_pyspark_schema.type(assignment.field.type)})"
        if self._needs_alias(assignment):
            return f"{expression}.alias({self._literal(assignment.field.column)})"
        return expression

    def _needs_cast(self, assignment) -> bool:
        if isinstance(assignment.field.type, StructType):
            return False
        if self._window_rank_expression(assignment.expression):
            return True
        if assignment.expression.type is None:
            return True
        if not self._same_type(assignment.expression.type, assignment.field.type):
            return True
        return assignment.expression.kind == "sub" and isinstance(assignment.field.type, DecimalType)

    def _window_rank_expression(self, expression: PySparkExpressionRecipe) -> bool:
        if expression.kind != "reserved_v2":
            return False
        return expression.data.get("function") in {"window_row_number", "window_rank", "window_dense_rank"}

    def _needs_alias(self, assignment) -> bool:
        if assignment.expression.kind != "field":
            return True
        return assignment.expression.data["field"] != assignment.field.column

    def _same_type(self, actual: StructureType, target: StructureType) -> bool:
        if actual.name != target.name:
            return False
        if isinstance(actual, DecimalType) and isinstance(target, DecimalType):
            return actual.precision == target.precision and actual.scale == target.scale
        return actual == target or actual.__class__.__name__.removesuffix("Type") == target.__class__.__name__

    def _target(self, step: PySparkStepRecipe | PySparkOutputRecipe) -> str:
        if isinstance(step, PySparkStepRecipe):
            return step.results[0].frame
        return step.name

    def _validations(
        self,
        validations: tuple[PySparkValidationRecipe, ...],
        *,
        target: str = "df",
    ) -> list[str]:
        lines: list[str] = []
        for validation in validations:
            schema = render_pyspark_schema.constant_name(validation.schema)
            lines.append(
                f'        assert_schema({target}, {schema}, '
                f'name="{validation.schema.__name__}", mode="{validation.mode.value}")'
            )
            if validation.project:
                lines.append(f"        {target} = project_schema({target}, {schema})")
        return lines

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

    def _literal(self, value: str) -> str:
        return json.dumps(value)


render_pyspark_step = RenderPySparkStep()
