from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.app.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.target.pyspark.model.PySparkStepResultRecipe import PySparkStepResultRecipe
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkStepRecipe:
    name: str
    ordinal: int
    source: str
    source_scope: str
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
    results: tuple[PySparkStepResultRecipe, ...] = ()
    operations: tuple[PySparkOperationRecipe, ...] = ()
