from __future__ import annotations

from structure.plugin.api.v1.model import StreamingFinding
from structure.plugin.pyspark.compiler.model.PySparkMapGeneratorRecipe import PySparkMapGeneratorRecipe
from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe
from structure.plugin.pyspark.compiler.model.PySparkScalarGeneratorRecipe import PySparkScalarGeneratorRecipe


class ClassifyGeneratorStreamingCompatibility:
    """Classify typed generator operations for caller-owned streaming use."""

    def posexplode_struct(
        self,
        step: str,
        generator: PySparkPosexplodeStructRecipe,
    ) -> tuple[StreamingFinding, ...]:
        return ()

    def scalar_array(
        self,
        step: str,
        generator: PySparkScalarGeneratorRecipe,
    ) -> tuple[StreamingFinding, ...]:
        return ()

    def map(
        self,
        step: str,
        generator: PySparkMapGeneratorRecipe,
    ) -> tuple[StreamingFinding, ...]:
        return ()
