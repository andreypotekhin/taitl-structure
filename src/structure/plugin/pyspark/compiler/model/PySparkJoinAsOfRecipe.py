from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.dsl.joins import AsOf, TiePolicy


@dataclass(frozen=True)
class PySparkJoinAsOfRecipe:
    left_time: PySparkExpressionRecipe
    right_time: PySparkExpressionRecipe
    direction: AsOf
    tolerance: PySparkExpressionRecipe | None = None
    ties: TiePolicy = TiePolicy.ERROR
