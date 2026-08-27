from dataclasses import dataclass

from structure.plugin.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe


@dataclass(frozen=True)
class PySparkStageOutputRecipe:
    """A lowered composed-stage output addressed by a recursive path."""

    path: tuple[str, ...]
    output: PySparkOutputRecipe
