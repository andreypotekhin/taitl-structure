from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkStackRecipe:
    rows: int
    values: tuple[PySparkExpressionRecipe, ...]
    scope: str
    schema: type[Schema]
