from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.target.pyspark.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.core.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.core.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.core.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


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
