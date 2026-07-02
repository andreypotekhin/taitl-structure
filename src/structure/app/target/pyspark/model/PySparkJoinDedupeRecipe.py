from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.transforms.TiePolicy import TiePolicy
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkJoinDedupeRecipe:
    order_by: PySparkExpressionRecipe
    direction: str
    ties: TiePolicy
