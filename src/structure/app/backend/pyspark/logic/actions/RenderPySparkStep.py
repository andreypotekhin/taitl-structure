from __future__ import annotations

from structure.app.backend.pyspark.logic.actions.RenderPySparkExpression import render_pyspark_expression
from structure.app.backend.pyspark.logic.actions.RenderPySparkSchema import render_pyspark_schema
from structure.app.backend.pyspark.logic.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.backend.pyspark.logic.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.backend.pyspark.logic.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.backend.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe


class RenderPySparkStep:

    def __call__(self, step: PySparkStepRecipe, *, current: str) -> str:
        lines = [f"        # Subtransform: {step.name}"]
        active = current
        if step.before_hooks:
            lines.extend(self._hooks(step.before_hooks, current=current))
            active = "df"
        lines.append(f'        df = {active}.alias("{step.input_alias}")')
        lines.extend(self._joins(step))
        if step.filters:
            lines.extend(self._filters(step))
        lines.extend(self._projection(step))
        lines.extend(self._hooks(step.after_hooks, current="df"))
        lines.extend(self._validations(step.validations))
        return "\n".join(lines)

    def _hooks(self, hooks: tuple[PySparkHookRecipe, ...], *, current: str) -> list[str]:
        lines: list[str] = []
        for hook in hooks:
            inputs = ", inputs=inputs" if hook.pass_inputs else ""
            lines.append(f"        df = self._impl.{hook.name}(df={current}{inputs}, spark=self.spark, ctx=self.ctx)")
            current = "df"
        return lines

    def _joins(self, step: PySparkStepRecipe) -> list[str]:
        lines: list[str] = []
        for join in step.joins:
            right = f'{join.input_name}.alias("{join.right_alias}")'
            if join.hint is not None and join.hint.value == "broadcast":
                right = f"F.broadcast({right})"
            lines.append(f"        {join.input_name}_df = {right}")
            predicate = render_pyspark_expression(join.predicate, scope_aliases=self._scope_aliases(step, join))
            lines.append("        df = df.join(")
            lines.append(f"            {join.input_name}_df,")
            lines.append(f"            {predicate},")
            lines.append(f'            "{join.how.value}",')
            lines.append("        )")
        return lines

    def _filters(self, step: PySparkStepRecipe) -> list[str]:
        predicate = " & ".join(
            f"({render_pyspark_expression(filter, scope_aliases=self._scope_aliases(step))})" for filter in step.filters
        )
        return [f"        df = df.where({predicate})"]

    def _projection(self, step: PySparkStepRecipe) -> list[str]:
        lines = ["        df = df.select("]
        for assignment in step.projection:
            expression = render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))
            lines.append(f'            {expression}.alias("{assignment.field.name}"),')
        lines.append("        )")
        return lines

    def _validations(self, validations: tuple[PySparkValidationRecipe, ...]) -> list[str]:
        lines: list[str] = []
        for validation in validations:
            schema = render_pyspark_schema.constant_name(validation.schema)
            lines.append(
                f'        assert_schema(df, {schema}, name="{validation.schema.__name__}", mode="{validation.mode.value}")'
            )
            if validation.project:
                lines.append(f"        df = project_schema(df, {schema})")
        return lines

    def _scope_aliases(self, step: PySparkStepRecipe, join: PySparkJoinRecipe | None = None) -> dict[str, str]:
        aliases = {
            step.input_schema.__name__: step.input_alias,
        }
        if step.ordinal == 0:
            aliases["orders"] = step.input_alias
        for item in step.joins:
            aliases[item.input_name] = item.right_alias
        if join is not None:
            aliases[join.input_name] = join.right_alias
        return aliases


render_pyspark_step = RenderPySparkStep()
