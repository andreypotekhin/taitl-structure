from dataclasses import dataclass

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkRelationRepartitionRecipe:
    partitions: int | None
    keys: tuple[PySparkExpressionRecipe, ...]
