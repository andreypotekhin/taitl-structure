from __future__ import annotations

import json

from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.plugin.pyspark.compiler.model.PySparkScalarGeneratorRecipe import PySparkScalarGeneratorRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression


class RenderPySparkScalarGenerator:
    """Render native Spark generators for arrays of primitive values."""

    def __call__(
        self,
        generator: PySparkScalarGeneratorRecipe,
        *,
        aliases: dict[str, str],
        target: str,
        index: int,
    ) -> list[str]:
        value = render_pyspark_expression(generator.expression, scope_aliases=aliases)
        prefix = f"__structure_{self._identifier(generator.scope)}_{index}"
        position = f"{prefix}_pos"
        item = f"{prefix}_item"
        function = f"F.{generator.function}"
        aliases_text = (
            f"{self._literal(position)}, {self._literal(item)}"
            if generator.ordinal is not None
            else self._literal(item)
        )
        lines = [
            f"        {target} = {target}.select(",
            '            "*",',
            f"            {function}({value}).alias({aliases_text}),",
            "        )",
        ]
        value_column = generator.schema._structure_fields[generator.value_field].column
        lines.extend(
            [
                f"        {target} = {target}.withColumn(",
                f"            {self._literal(value_column)},",
                f"            F.col({self._literal(item)}),",
                "        )",
            ]
        )
        if generator.ordinal is not None:
            ordinal_column = generator.schema._structure_fields[generator.ordinal].column
            lines.extend(
                [
                    f"        {target} = {target}.withColumn(",
                    f"            {self._literal(ordinal_column)},",
                    f"            F.col({self._literal(position)}).cast(T.LongType()),",
                    "        )",
                ]
            )
        lines.append(f"        {target} = {target}.drop({', '.join(self._literal(name) for name in (position, item))})")
        return lines

    def aliases(self, step: PySparkStepRecipe | PySparkOutputRecipe) -> dict[str, str]:
        return {
            operation.scalar_generator.scope: ""
            for operation in step.operations
            if operation.scalar_generator is not None
        }

    @staticmethod
    def _identifier(value: str) -> str:
        clean = "".join(character if character.isalnum() else "_" for character in value)
        return clean.strip("_") or "scope"

    @staticmethod
    def _literal(value: str) -> str:
        return json.dumps(value)
