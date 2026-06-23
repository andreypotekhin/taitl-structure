from __future__ import annotations

from structure.app.target.pyspark.logic.actions.RenderPySparkExpression import render_pyspark_expression
from structure.app.target.pyspark.logic.actions.RenderPySparkSchema import render_pyspark_schema
from structure.app.target.pyspark.logic.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.logic.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.logic.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.app.target.pyspark.logic.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.target.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe


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
        lines = [f"        # Subtransform: {step.name}"]
        active = current
        if step.before_hooks:
            lines.extend(self._hooks(step.before_hooks, current=current))
            active = "df"
        lines.append(f'        df = {active}.alias("{step.input_alias}")')
        lines.extend(self._joins(step, sources=sources or {}))
        if step.filters:
            lines.extend(self._filters(step))
        lines.extend(self._projection(step))
        lines.extend(self._hooks(step.after_hooks, current="df"))
        lines.extend(self._validations(step.validations))
        return "\n".join(lines)

    def _multiple(self, step: PySparkStepRecipe, *, current: str, sources: dict[str, str]) -> str:
        lines = [f"        # Subtransform: {step.name}"]
        active = current
        if step.before_hooks:
            lines.extend(self._hooks(step.before_hooks, current=current))
            active = "df"
        base = f"{step.name}_base"
        lines.append(f'        {base} = {active}.alias("{step.input_alias}")')
        lines.extend(self._joins(step, sources=sources, target=base))
        if step.filters:
            lines.extend(self._filters(step, target=base))
        for result in step.results:
            lines.extend(self._result(step, result, base=base))
        return "\n".join(lines)

    def _result(self, step: PySparkStepRecipe, result, *, base: str) -> list[str]:
        lines = [f"        {result.frame} = {base}.select("]
        for assignment in result.projection:
            expression = render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))
            lines.append(f'            {expression}.alias("{assignment.field.name}"),')
        lines.append("        )")
        lines.extend(self._hooks(result.after_hooks, current=result.frame, target=result.frame))
        lines.extend(self._validations(result.validations, target=result.frame))
        return lines

    def _hooks(
        self,
        hooks: tuple[PySparkHookRecipe, ...],
        *,
        current: str,
        target: str = "df",
    ) -> list[str]:
        lines: list[str] = []
        for hook in hooks:
            inputs = ", inputs=inputs" if hook.pass_inputs else ""
            lines.append(
                f"        {target} = self._impl.{hook.name}" f"(df={current}{inputs}, spark=self.spark, ctx=self.ctx)"
            )
            current = target
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
            source = sources.get(join.source, join.source)
            right = f'{source}.alias("{join.right_alias}")'
            if join.hint is not None and join.hint.value == "broadcast":
                right = f"F.broadcast({right})"
            lines.append(f"        {join.input_name}_df = {right}")
            predicate = render_pyspark_expression(join.predicate, scope_aliases=self._scope_aliases(step, join))
            lines.append(f"        {target} = {target}.join(")
            lines.append(f"            {join.input_name}_df,")
            lines.append(f"            {predicate},")
            lines.append(f'            "{join.how.value}",')
            lines.append("        )")
        return lines

    def _filters(self, step: PySparkStepRecipe | PySparkOutputRecipe, *, target: str = "df") -> list[str]:
        predicate = " & ".join(
            f"({render_pyspark_expression(filter, scope_aliases=self._scope_aliases(step))})" for filter in step.filters
        )
        return [f"        {target} = {target}.where({predicate})"]

    def _projection(self, step: PySparkStepRecipe | PySparkOutputRecipe) -> list[str]:
        if not step.projection:
            return []
        lines = ["        df = df.select("]
        for assignment in step.projection:
            expression = render_pyspark_expression(assignment.expression, scope_aliases=self._scope_aliases(step))
            lines.append(f'            {expression}.alias("{assignment.field.name}"),')
        lines.append("        )")
        return lines

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


render_pyspark_step = RenderPySparkStep()
