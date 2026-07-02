from __future__ import annotations

from dataclasses import dataclass

from structure.app.target.pyspark.model.PySparkAggregateAssignment import PySparkAggregateAssignment
from structure.app.target.pyspark.model.PySparkAggregateKey import PySparkAggregateKey


@dataclass(frozen=True)
class PySparkAggregateRecipe:
    keys: tuple[PySparkAggregateKey, ...]
    assignments: tuple[PySparkAggregateAssignment, ...]
