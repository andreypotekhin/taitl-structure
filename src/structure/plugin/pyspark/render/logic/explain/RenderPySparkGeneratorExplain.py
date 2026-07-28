from __future__ import annotations

from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe


class RenderPySparkGeneratorExplain:
    """Render explain text for typed generator operations."""

    def posexplode_struct(self, generator: PySparkPosexplodeStructRecipe) -> str:
        return f"{generator.function}_struct(row_multiplying scope={generator.scope} schema={generator.schema.__name__})"
