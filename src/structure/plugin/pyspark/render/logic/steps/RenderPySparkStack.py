from __future__ import annotations

from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.plugin.pyspark.compiler.model.PySparkStackRecipe import PySparkStackRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression


class RenderPySparkStack:
    """Render a declared-schema stack generator using native Spark SQL expressions."""

    def __call__(
        self,
        stack: PySparkStackRecipe,
        *,
        aliases: dict[str, str],
        target: str,
    ) -> list[str]:
        values = ", ".join(render_pyspark_expression(value, scope_aliases=aliases) for value in stack.values)
        columns = ", ".join(repr(field.column) for field in stack.schema._structure_fields.values())
        return [
            f"        {target} = {target}.select(",
            '            "*",',
            f"            F.stack(F.lit({stack.rows}), {values}).alias({columns}),",
            "        )",
        ]

    def aliases(self, step: PySparkStepRecipe | PySparkOutputRecipe) -> dict[str, str]:
        return {operation.stack.scope: "" for operation in step.operations if operation.stack is not None}
