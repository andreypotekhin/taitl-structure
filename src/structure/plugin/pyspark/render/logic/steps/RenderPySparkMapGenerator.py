from __future__ import annotations

import json

from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression


class RenderPySparkMapGenerator:
    """Render native Spark generators for primitive maps."""

    def __call__(self, generator, *, aliases, target, index) -> list[str]:
        value = render_pyspark_expression(generator.expression, scope_aliases=aliases)
        prefix = f"__structure_{self._identifier(generator.scope)}_{index}"
        position, key, item = f"{prefix}_pos", f"{prefix}_key", f"{prefix}_value"
        aliases_text = ", ".join(
            self._literal(name) for name in ((position, key, item) if generator.ordinal is not None else (key, item))
        )
        lines = [
            f"        {target} = {target}.select(",
            '            "*",',
            f"            F.{generator.function}({value}).alias({aliases_text}),",
            "        )",
        ]
        for temporary, field_name in ((key, generator.key_field), (item, generator.value_field)):
            lines.extend(
                [
                    f"        {target} = {target}.withColumn(",
                    f"            {self._literal(generator.schema._structure_fields[field_name].column)},",
                    f"            F.col({self._literal(temporary)}),",
                    "        )",
                ]
            )
        if generator.ordinal is not None:
            lines.extend(
                [
                    f"        {target} = {target}.withColumn(",
                    f"            {self._literal(generator.schema._structure_fields[generator.ordinal].column)},",
                    f"            F.col({self._literal(position)}).cast(T.LongType()),",
                    "        )",
                ]
            )
        lines.append(
            f"        {target} = {target}.drop({', '.join(self._literal(name) for name in ((position, key, item) if generator.ordinal is not None else (key, item)))})"
        )
        return lines

    def aliases(self, step: PySparkStepRecipe | PySparkOutputRecipe) -> dict[str, str]:
        return {
            operation.map_generator.scope: "" for operation in step.operations if operation.map_generator is not None
        }

    @staticmethod
    def _identifier(value: str) -> str:
        clean = "".join(character if character.isalnum() else "_" for character in value)
        return clean.strip("_") or "scope"

    @staticmethod
    def _literal(value: str) -> str:
        return json.dumps(value)
