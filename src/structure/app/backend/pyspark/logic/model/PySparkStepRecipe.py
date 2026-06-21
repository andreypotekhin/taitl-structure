from __future__ import annotations

from dataclasses import dataclass

from structure.app.backend.pyspark.logic.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.backend.pyspark.logic.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.backend.pyspark.logic.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.backend.pyspark.logic.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.backend.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe
from structure.app.dsl.logic.model.schemas.Structure import Structure


@dataclass(frozen=True)
class PySparkStepRecipe:
    name: str
    ordinal: int
    input_schema: type[Structure]
    output_schema: type[Structure]
    input_alias: str
    output_alias: str
    before_hooks: tuple[PySparkHookRecipe, ...]
    filters: tuple[PySparkExpressionRecipe, ...]
    joins: tuple[PySparkJoinRecipe, ...]
    projection: tuple[PySparkProjectionRecipe, ...]
    after_hooks: tuple[PySparkHookRecipe, ...]
    validations: tuple[PySparkValidationRecipe, ...]
