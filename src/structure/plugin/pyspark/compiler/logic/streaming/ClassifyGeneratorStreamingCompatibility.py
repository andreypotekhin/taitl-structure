from __future__ import annotations

from structure.plugin.api.v1.model import StreamingFinding
from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe
from structure.plugin.pyspark.dsl.operations import StreamingSupport


class ClassifyGeneratorStreamingCompatibility:
    """Classify typed generator operations for caller-owned streaming use."""

    def posexplode_struct(
        self,
        step: str,
        generator: PySparkPosexplodeStructRecipe,
    ) -> tuple[StreamingFinding, ...]:
        return (
            StreamingFinding(
                code="STREAM-E0801",
                support=StreamingSupport.BATCH_ONLY,
                step=step,
                operation=f"{generator.function}_struct {generator.scope}",
                problem=(
                    f"{generator.function}_struct(...) is row-expanding and is batch-only until generator streaming "
                    "semantics are admitted."
                ),
                use="Keep this transform batch-only or perform row expansion before the streaming transform.",
            ),
        )
