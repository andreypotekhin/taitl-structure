from __future__ import annotations

from dataclasses import dataclass

from structure.platform.pyspark.model.PySparkAggregateAssignment import PySparkAggregateAssignment
from structure.platform.pyspark.model.PySparkAggregateKey import PySparkAggregateKey
from structure.platform.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkAggregateRecipe:
    keys: tuple[PySparkAggregateKey, ...]
    assignments: tuple[PySparkAggregateAssignment, ...]
    grouping: str = "group_by"
    levels: tuple[tuple[str, ...], ...] = ()
    having: PySparkExpressionRecipe | None = None
