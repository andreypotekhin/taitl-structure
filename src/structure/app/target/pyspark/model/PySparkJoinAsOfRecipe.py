from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.transforms.AsOf import AsOf
from structure.app.dsl.model.transforms.TiePolicy import TiePolicy
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkJoinAsOfRecipe:
    left_time: PySparkExpressionRecipe
    right_time: PySparkExpressionRecipe
    direction: AsOf
    tolerance: PySparkExpressionRecipe | None = None
    ties: TiePolicy = TiePolicy.ERROR
