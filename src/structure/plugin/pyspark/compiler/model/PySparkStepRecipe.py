from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.api.v1 import TransformMemberOrigin
from structure.plugin.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.plugin.pyspark.compiler.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepResultRecipe import PySparkStepResultRecipe
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkStepRecipe:
    name: str
    ordinal: int
    source: str
    source_scope: str
    input_schema: type[Schema]
    output_schema: type[Schema]
    input_alias: str
    output_alias: str
    before_hooks: tuple[PySparkHookRecipe, ...]
    filters: tuple[PySparkExpressionRecipe, ...]
    joins: tuple[PySparkJoinRecipe, ...]
    projection: tuple[PySparkProjectionRecipe, ...]
    after_hooks: tuple[PySparkHookRecipe, ...]
    validations: tuple[PySparkValidationRecipe, ...]
    aggregate: PySparkAggregateRecipe | None = None
    results: tuple[PySparkStepResultRecipe, ...] = ()
    operations: tuple[PySparkOperationRecipe, ...] = ()
    origin: TransformMemberOrigin | None = None
