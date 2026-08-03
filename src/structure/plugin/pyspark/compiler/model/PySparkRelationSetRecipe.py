from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkRelationSetRecipe:
    operation: str
    input_name: str
    source: str
    schema: type[Schema]
    by_name: bool
    allow_missing_columns: bool = False
    defaults: tuple[tuple[str, PySparkExpressionRecipe], ...] = ()
