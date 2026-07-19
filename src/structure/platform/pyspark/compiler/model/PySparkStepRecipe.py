from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema
from structure.platform.api.v1 import TransformMemberOrigin
from structure.platform.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.platform.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.platform.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.platform.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.platform.pyspark.compiler.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.platform.pyspark.compiler.model.PySparkStepResultRecipe import PySparkStepResultRecipe
from structure.platform.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


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
