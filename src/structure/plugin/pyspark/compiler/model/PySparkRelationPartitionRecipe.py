from dataclasses import dataclass

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkRelationPartitionRecipe:
    count: int
    order_by: tuple[PySparkExpressionRecipe, ...]
