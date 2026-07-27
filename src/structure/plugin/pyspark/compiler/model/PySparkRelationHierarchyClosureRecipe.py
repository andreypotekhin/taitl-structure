from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkRelationHierarchyClosureRecipe:
    id: PySparkExpressionRecipe
    parent: PySparkExpressionRecipe
    schema: type[Schema]
    scope: str
    node: str
    ancestor: str
    depth: str
    max_depth: int
