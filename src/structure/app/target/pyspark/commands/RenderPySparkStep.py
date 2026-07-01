from __future__ import annotations

import json

from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.StructType import StructType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.target.pyspark.commands.RenderPySparkExpression import render_pyspark_expression
from structure.app.target.pyspark.commands.RenderPySparkSchema import render_pyspark_schema
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkOutputRecipe import PySparkOutputRecipe
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
        if pending_filters:
            ordered_lines.extend(self._filters(tuple(pending_filters), step=step, target=target))
        return ordered_lines

    def _join(
        self,
        step: PySparkStepRecipe | PySparkOutputRecipe,
        join: PySparkJoinRecipe,
        *,
        sources: dict[str, str],
        target: str,
    ) -> list[str]:
        source = sources.get(join.source, join.source)
        right = f'{source}.alias("{join.right_alias}")'
        if join.hint is not None and join.hint.value == "broadcast":
            right = f"F.broadcast({right})"
        predicate = render_pyspark_expression(join.predicate, scope_aliases=self._scope_aliases(step, join))
        right_name = f"{join.right_alias}_joined"
        return [
            f"        {right_name} = {right}",
            f"        {target} = {target}.join(",
            f"            {right_name},",
            f"            {predicate},",
            f'            "{self._join_mode(join)}",',
            "        )",
        ]

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
        if assignment.expression.type is None:
            return True
        if not self._same_type(assignment.expression.type, assignment.field.type):
            return True
        return assignment.expression.kind == "sub" and isinstance(assignment.field.type, DecimalType)

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
