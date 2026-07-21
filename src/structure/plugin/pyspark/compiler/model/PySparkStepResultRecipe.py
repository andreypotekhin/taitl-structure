from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.plugin.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.plugin.pyspark.compiler.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


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
