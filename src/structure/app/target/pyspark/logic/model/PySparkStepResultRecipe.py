from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.target.pyspark.logic.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.logic.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.target.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkStepResultRecipe:
    schema: type[Structure]
    lane: str
    frame: str
    output_alias: str
    projection: tuple[PySparkProjectionRecipe, ...]
    ordinal: int
    after_hooks: tuple[PySparkHookRecipe, ...]
    validations: tuple[PySparkValidationRecipe, ...]
