from __future__ import annotations

from dataclasses import dataclass

from structure.app.backend.pyspark.logic.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.dsl.logic.model.schemas.FieldDefinition import FieldDefinition


@dataclass(frozen=True)
class PySparkProjectionRecipe:
    field: FieldDefinition
    expression: PySparkExpressionRecipe
