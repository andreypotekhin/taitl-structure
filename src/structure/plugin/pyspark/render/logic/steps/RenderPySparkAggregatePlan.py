from structure.plugin.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression


class RenderPySparkAggregatePlan:
    """Render aggregate-plan control flow while the step renderer owns shared primitives."""

    def __init__(self, step_renderer) -> None:
        self._step = step_renderer

    def __call__(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        aggregate: PySparkAggregateRecipe,
        *,
        target: str,
    ) -> list[str]:
        if aggregate.grouping == "grouping_sets":
            return self._grouping_sets(step, aggregate, target=target)
        grouping = {"group_by": "groupBy", "rollup": "rollup", "cube": "cube"}.get(aggregate.grouping)
        if grouping is None:
            raise TypeError(f"Unsupported aggregate grouping: {aggregate.grouping}")
        key_columns = self._step._aggregate_key_columns(aggregate) if aggregate.grouping in {"rollup", "cube"} else ()
        lines: list[str] = []
        if aggregate.grouping == "group_by" and not aggregate.keys:
            lines.append(f"        {target} = {target}.agg(")
            for assignment in aggregate.assignments:
                if assignment.function != "key":
                    lines.append(f"            {self._step._aggregate_assignment(assignment, step=step, aggregate=aggregate, key_columns=key_columns)},")
            lines.append("        ).select(")
            for assignment in aggregate.assignments:
                lines.append(f"            {self._step._aggregate_select(assignment, key_columns=key_columns)},")
            lines.append("        )")
            lines.extend(self._step._aggregate_having(step, aggregate, target=target))
            return lines
        if aggregate.grouping in {"rollup", "cube"}:
            for key, column in key_columns:
                expression = render_pyspark_expression(key.expression, scope_aliases=self._step._scope_aliases(step))
                lines.append(f"        {target} = {target}.withColumn({self._step._literal(column)}, {expression})")
        lines.append(f"        {target} = {target}.{grouping}(")
        for key in aggregate.keys:
            if aggregate.grouping in {"rollup", "cube"}:
                lines.append(f"            {self._step._literal(self._step._aggregate_key_column(key, key_columns))},")
            else:
                expression = render_pyspark_expression(key.expression, scope_aliases=self._step._scope_aliases(step))
                lines.append(f"            {expression}.alias({self._step._literal(key.name)}),")
        lines.append("        ).agg(")
        for assignment in aggregate.assignments:
            if assignment.function != "key":
                lines.append(f"            {self._step._aggregate_assignment(assignment, step=step, aggregate=aggregate, key_columns=key_columns)},")
        lines.append("        ).select(")
        for assignment in aggregate.assignments:
            lines.append(f"            {self._step._aggregate_select(assignment, key_columns=key_columns)},")
        lines.append("        )")
        lines.extend(self._step._aggregate_having(step, aggregate, target=target))
        return lines

    def _grouping_sets(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        aggregate: PySparkAggregateRecipe,
        *,
        target: str,
    ) -> list[str]:
        key_columns = self._step._aggregate_key_columns(aggregate)
        lines: list[str] = []
        for key, column in key_columns:
            expression = render_pyspark_expression(key.expression, scope_aliases=self._step._scope_aliases(step))
            lines.append(f"        {target} = {target}.withColumn({self._step._literal(column)}, {expression})")
        branches: list[str] = []
        for index, level in enumerate(aggregate.levels, start=1):
            branch = f"{target}_grouping_set_{index}"
            branches.append(branch)
            level_keys = set(level)
            lines.append(f"        {branch} = {target}.groupBy(")
            for key in aggregate.keys:
                if key.name in level_keys:
                    lines.append(f"            {self._step._literal(self._step._aggregate_key_column(key, key_columns))},")
            lines.append("        ).agg(")
            for assignment in aggregate.assignments:
                if assignment.function not in {"key", "grouping_id", "is_grouped"}:
                    lines.append(f"            {self._step._aggregate_assignment(assignment, step=step, aggregate=aggregate, key_columns=key_columns)},")
            lines.append("        ).select(")
            for assignment in aggregate.assignments:
                lines.append(f"            {self._step._grouping_set_select(assignment, aggregate=aggregate, level=level_keys, key_columns=key_columns)},")
            lines.append("        )")
        if not branches:
            raise TypeError("grouping_sets(...) requires at least one grouping level")
        lines.append(f"        {target} = {branches[0]}")
        for branch in branches[1:]:
            lines.append(f"        {target} = {target}.unionByName({branch})")
        lines.extend(self._step._aggregate_having(step, aggregate, target=target))
        return lines
