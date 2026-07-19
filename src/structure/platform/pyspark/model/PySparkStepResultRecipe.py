from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema
from structure.platform.pyspark.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.platform.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.platform.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.platform.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkStepResultRecipe:
    schema: type[Schema]
    lane: str
    frame: str
    output_alias: str
    projection: tuple[PySparkProjectionRecipe, ...]
    ordinal: int
    after_hooks: tuple[PySparkHookRecipe, ...]
    validations: tuple[PySparkValidationRecipe, ...]
    aggregate: PySparkAggregateRecipe | None = None
