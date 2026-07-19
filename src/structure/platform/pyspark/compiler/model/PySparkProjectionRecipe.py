from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.FieldDefinition import FieldDefinition
from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkProjectionRecipe:
    field: FieldDefinition
    expression: PySparkExpressionRecipe
