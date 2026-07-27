from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkRelationHierarchyFallbackRecipe:
    source_id: PySparkExpressionRecipe
    path: PySparkExpressionRecipe
    parent_input: str
    parent_source: str
    parent_schema: type[Schema]
    parent_id: PySparkExpressionRecipe
    parent: PySparkExpressionRecipe
    schema: type[Schema]
    scope: str
    source: str
    fallback: str
    ordinal: str
    separator: str
    max_depth: int
