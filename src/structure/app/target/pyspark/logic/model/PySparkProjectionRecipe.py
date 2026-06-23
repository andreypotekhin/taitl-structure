from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.schemas.FieldDefinition import FieldDefinition
from structure.app.target.pyspark.logic.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkProjectionRecipe:
    field: FieldDefinition
    expression: PySparkExpressionRecipe
