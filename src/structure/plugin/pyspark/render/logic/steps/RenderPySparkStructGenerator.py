from __future__ import annotations

import json

from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import render_pyspark_expression


class RenderPySparkStructGenerator:
    """Render typed array-of-struct generator operations."""

    def __call__(
        self,
        generator: PySparkPosexplodeStructRecipe,
        *,
        aliases: dict[str, str],
        target: str,
        index: int,
    ) -> list[str]:
        value = render_pyspark_expression(generator.expression, scope_aliases=aliases)
        prefix = f"__structure_{self._identifier(generator.scope)}_{index}"
        position = f"{prefix}_pos"
        item = f"{prefix}_item"
        lines = [
            f"        {target} = {target}.select(",
            '            "*",',
            f"            {self._generator_expression(generator, value, position, item)},",
            "        )",
        ]
        if generator.ordinal is not None:
            lines.extend(
                [
                    f"        {target} = {target}.withColumn(",
                    f"            {self._literal(generator.schema._structure_fields[generator.ordinal].column)},",
                    f"            F.col({self._literal(position)}).cast(T.LongType()),",
                    "        )",
                ]
            )
        for name, field in generator.schema._structure_fields.items():
            if name == generator.ordinal:
                continue
            lines.extend(
                [
                    f"        {target} = {target}.withColumn(",
                    f"            {self._literal(field.column)},",
                    f"            F.col({self._literal(f'{item}.{field.column}')}),",
                    "        )",
                ]
            )
        lines.append(f"        {target} = {target}.drop({self._drop_args(generator, position, item)})")
        return lines

    def aliases(self, step: PySparkStepRecipe | PySparkOutputRecipe) -> dict[str, str]:
        aliases: dict[str, str] = {}
        for operation in step.operations:
            if operation.posexplode_struct is not None:
                aliases[operation.posexplode_struct.scope] = ""
        return aliases

    def _identifier(self, value: str) -> str:
        clean = "".join(character if character.isalnum() else "_" for character in value)
        return clean.strip("_") or "scope"

    def _literal(self, value: str) -> str:
        return json.dumps(value)

    def _generator_expression(
        self,
        generator: PySparkPosexplodeStructRecipe,
        value: str,
        position: str,
        item: str,
    ) -> str:
        if generator.function == "explode":
            return f"F.explode({value}).alias({self._literal(item)})"
        return f"F.posexplode({value}).alias({self._literal(position)}, {self._literal(item)})"

    def _drop_args(self, generator: PySparkPosexplodeStructRecipe, position: str, item: str) -> str:
        if generator.ordinal is None:
            return self._literal(item)
        return f"{self._literal(position)}, {self._literal(item)}"
