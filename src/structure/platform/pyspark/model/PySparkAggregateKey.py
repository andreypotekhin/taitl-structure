from __future__ import annotations

from dataclasses import dataclass

from structure.platform.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkAggregateKey:
    name: str
    expression: PySparkExpressionRecipe
