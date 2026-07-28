from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.dsl.joins import TiePolicy


@dataclass(frozen=True)
class PySparkOrderedTimelineScanRecipe:
    scope: str
    state_scope: str
    row_scope: str
    state_schema: type[Schema]
    initial: tuple[tuple[str, PySparkExpressionRecipe], ...]
    transition: tuple[tuple[str, PySparkExpressionRecipe], ...]
    partition_by: tuple[PySparkExpressionRecipe, ...]
    order_by: tuple[PySparkExpressionRecipe, ...]
    max_rows: int
    ties: TiePolicy
