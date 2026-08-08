from __future__ import annotations

from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe
from structure.plugin.pyspark.compiler.model.PySparkScalarGeneratorRecipe import PySparkScalarGeneratorRecipe


class RenderPySparkGeneratorExplain:
    """Render explain text for typed generator operations."""

    def posexplode_struct(self, generator: PySparkPosexplodeStructRecipe) -> str:
        return (
            f"{generator.function}_struct(row_multiplying scope={generator.scope} schema={generator.schema.__name__})"
        )

    def scalar_array(self, generator: PySparkScalarGeneratorRecipe) -> str:
        return f"{generator.function}_array(row_multiplying scope={generator.scope} schema={generator.schema.__name__})"
