from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.transforms.OverlapPolicy import OverlapPolicy
from structure.core.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkJoinTemporalRecipe:
    at: PySparkExpressionRecipe
    valid_from: PySparkExpressionRecipe
    valid_to: PySparkExpressionRecipe
    overlaps: OverlapPolicy = OverlapPolicy.ERROR
