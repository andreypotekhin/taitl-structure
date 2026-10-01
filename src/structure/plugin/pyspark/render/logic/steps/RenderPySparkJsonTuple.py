from __future__ import annotations

from structure.plugin.pyspark.compiler.model.PySparkJsonTupleRecipe import PySparkJsonTupleRecipe
from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression


class RenderPySparkJsonTuple:
    """Render the row-preserving native Spark JSON tuple helper."""

    def __call__(
        self,
        generator: PySparkJsonTupleRecipe,
        *,
        aliases: dict[str, str],
        target: str,
    ) -> list[str]:
        value = render_pyspark_expression(generator.expression, scope_aliases=aliases)
        json_fields = ", ".join(repr(json_name) for _, json_name in generator.fields)
        output_fields = ", ".join(repr(generator.schema._structure_fields[name].column) for name, _ in generator.fields)
        return [
            f"        {target} = {target}.select(",
            '            "*",',
            f"            F.json_tuple({value}, {json_fields}).alias({output_fields}),",
            "        )",
        ]

    def aliases(self, step: PySparkStepRecipe | PySparkOutputRecipe) -> dict[str, str]:
        return {operation.json_tuple.scope: "" for operation in step.operations if operation.json_tuple is not None}
