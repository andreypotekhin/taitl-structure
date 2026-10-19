from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkObservationRecipe:
    name: str
    path: tuple[str, ...]
    schema: type[Schema]
    metrics: tuple[tuple[str, PySparkExpressionRecipe], ...]
