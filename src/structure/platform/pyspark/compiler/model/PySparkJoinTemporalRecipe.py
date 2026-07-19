from __future__ import annotations

from dataclasses import dataclass

from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.platform.pyspark.dsl.joins import OverlapPolicy


@dataclass(frozen=True)
class PySparkJoinTemporalRecipe:
    at: PySparkExpressionRecipe
    valid_from: PySparkExpressionRecipe
    valid_to: PySparkExpressionRecipe
    overlaps: OverlapPolicy = OverlapPolicy.ERROR
