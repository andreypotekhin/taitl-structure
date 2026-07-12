from __future__ import annotations

from dataclasses import dataclass

from structure.app.target.pyspark.model.PySparkAggregateAssignment import PySparkAggregateAssignment
from structure.app.target.pyspark.model.PySparkAggregateKey import PySparkAggregateKey
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkAggregateRecipe:
    keys: tuple[PySparkAggregateKey, ...]
    assignments: tuple[PySparkAggregateAssignment, ...]
    grouping: str = "group_by"
    levels: tuple[tuple[str, ...], ...] = ()
    having: PySparkExpressionRecipe | None = None
