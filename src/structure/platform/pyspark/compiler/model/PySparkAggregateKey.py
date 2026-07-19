from __future__ import annotations

from dataclasses import dataclass

from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkAggregateKey:
    name: str
    expression: PySparkExpressionRecipe
