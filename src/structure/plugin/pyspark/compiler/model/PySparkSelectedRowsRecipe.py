from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.dsl.joins import TiePolicy


@dataclass(frozen=True)
class PySparkSelectedRowsRecipe:
    direction: str
    order_by: PySparkExpressionRecipe
    partition_by: tuple[PySparkExpressionRecipe, ...]
    ties: TiePolicy
