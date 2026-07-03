from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.transforms.TiePolicy import TiePolicy
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkSelectedRowsRecipe:
    direction: str
    order_by: PySparkExpressionRecipe
    partition_by: tuple[PySparkExpressionRecipe, ...]
    ties: TiePolicy
