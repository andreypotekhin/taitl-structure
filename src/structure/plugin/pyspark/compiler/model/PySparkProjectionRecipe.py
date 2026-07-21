from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import FieldDefinition
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkProjectionRecipe:
    field: FieldDefinition
    expression: PySparkExpressionRecipe
