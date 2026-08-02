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
        if generator.tvf:
            return self._variant_tvf(generator, value, target=target, position=position, key=item + "_key", item=item)
        lines = [
            f"        {target} = {target}.select(",
            '            "*",',
            f"            {self._generator_expression(generator, value, position, item)},",
            "        )",
        ]
        if self._inline(generator):
            return [*lines, *self._inline_fields(target, generator, position)]
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

    def _variant_tvf(
        self,
        generator: PySparkPosexplodeStructRecipe,
        value: str,
        *,
        target: str,
        position: str,
        key: str,
        item: str,
    ) -> list[str]:
        function = generator.function
        lines = [
            f"        {target} = {target}.lateralJoin(",
            f"            self.spark.tvf.{function}({value}.outer()).select(",
            f'                F.col("pos").alias({self._literal(position)}),',
            f'                F.col("key").alias({self._literal(key)}),',
            f'                F.col("value").alias({self._literal(item)}),',
            "            ),",
            f'            how={self._literal("left" if generator.outer else "cross")},',
            "        )",
        ]
        for name, field in generator.schema._structure_fields.items():
            source = {"pos": position, "key": key, "value": item}[name]
            expression = f'F.col({self._literal(source)})'
            if getattr(field.type, "name", None) == "long":
                expression += ".cast(T.LongType())"
            lines.extend(
                [
                    f"        {target} = {target}.withColumn(",
                    f"            {self._literal(field.column)},",
                    f"            {expression},",
                    "        )",
                ]
            )
        lines.append(f"        {target} = {target}.drop({', '.join(self._literal(name) for name in (position, key, item))})")
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
        if generator.function == "explode_outer":
            return f"F.explode_outer({value}).alias({self._literal(item)})"
        if generator.function == "explode":
            return f"F.explode({value}).alias({self._literal(item)})"
        if generator.function == "posexplode_outer":
            return f"F.posexplode_outer({value}).alias({self._literal(position)}, {self._literal(item)})"
        if generator.function == "inline_outer":
            return f"F.inline_outer({value}).alias({self._inline_aliases(generator, prefix=position)})"
        if generator.function == "inline":
            return f"F.inline({value}).alias({self._inline_aliases(generator, prefix=position)})"
        return f"F.posexplode({value}).alias({self._literal(position)}, {self._literal(item)})"

    def _drop_args(self, generator: PySparkPosexplodeStructRecipe, position: str, item: str) -> str:
        if generator.ordinal is None:
            return self._literal(item)
        return f"{self._literal(position)}, {self._literal(item)}"

    def _inline(self, generator: PySparkPosexplodeStructRecipe) -> bool:
        return generator.function in {"inline", "inline_outer"}

    def _inline_fields(self, target: str, generator: PySparkPosexplodeStructRecipe, prefix: str) -> list[str]:
        temporaries = tuple(self._inline_column(prefix, field.column) for field in generator.schema._structure_fields.values())
        lines: list[str] = []
        for temporary, field in zip(temporaries, generator.schema._structure_fields.values(), strict=True):
            lines.extend(
                [
                    f"        {target} = {target}.withColumn(",
                    f"            {self._literal(field.column)},",
                    f"            F.col({self._literal(temporary)}),",
                    "        )",
                ]
            )
        lines.append(f"        {target} = {target}.drop({', '.join(self._literal(name) for name in temporaries)})")
        return lines

    def _inline_aliases(self, generator: PySparkPosexplodeStructRecipe, *, prefix: str) -> str:
        return ", ".join(
            self._literal(self._inline_column(prefix, field.column))
            for field in generator.schema._structure_fields.values()
        )

    def _inline_column(self, prefix: str, column: str) -> str:
        return f"{prefix}_{self._identifier(column)}"
