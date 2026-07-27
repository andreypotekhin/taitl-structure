from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkRelationAssertionRecipe:
    operation: str
    keys: tuple[PySparkExpressionRecipe, ...] = ()
    predicate: PySparkExpressionRecipe | None = None
    value: PySparkExpressionRecipe | None = None
    reference_input: str | None = None
    reference_source: str | None = None
    reference_schema: type[Schema] | None = None
    reference_key: PySparkExpressionRecipe | None = None
    parent: PySparkExpressionRecipe | None = None
    order_by: PySparkExpressionRecipe | None = None
    max_depth: int | None = None
    nulls: str = "allow"
