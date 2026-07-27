from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.dsl.joins import TiePolicy


@dataclass(frozen=True)
class PySparkRelationPrioritySelectionRecipe:
    keys: tuple[PySparkExpressionRecipe, ...]
    predicate: PySparkExpressionRecipe
    order_by: PySparkExpressionRecipe
    missing: str
    ties: TiePolicy
