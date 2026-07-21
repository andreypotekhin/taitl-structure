from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.dsl.joins import TiePolicy


@dataclass(frozen=True)
class PySparkJoinDedupeRecipe:
    order_by: PySparkExpressionRecipe
    direction: str
    ties: TiePolicy
