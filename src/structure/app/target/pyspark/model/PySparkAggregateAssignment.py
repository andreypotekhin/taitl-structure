from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.FieldDefinition import FieldDefinition
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkAggregateAssignment:
    field: FieldDefinition
    function: str
    expression: PySparkExpressionRecipe | None = None
    key: str | None = None
    arguments: tuple[PySparkExpressionRecipe, ...] = ()
    filter: PySparkExpressionRecipe | None = None
    order_by: PySparkExpressionRecipe | None = None
    options: tuple[tuple[str, object], ...] = ()
