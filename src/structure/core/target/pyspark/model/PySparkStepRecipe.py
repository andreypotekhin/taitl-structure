from __future__ import annotations

from dataclasses import dataclass

from structure.core.compiler.ir.model.TransformMemberOrigin import TransformMemberOrigin
from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.target.pyspark.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.core.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.core.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.core.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.core.target.pyspark.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.core.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.core.target.pyspark.model.PySparkStepResultRecipe import PySparkStepResultRecipe
from structure.core.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


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
