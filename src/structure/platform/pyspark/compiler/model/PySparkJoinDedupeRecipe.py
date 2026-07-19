from __future__ import annotations

from dataclasses import dataclass

from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.platform.pyspark.dsl.joins import TiePolicy


@dataclass(frozen=True)
class PySparkJoinDedupeRecipe:
    order_by: PySparkExpressionRecipe
    direction: str
    ties: TiePolicy
