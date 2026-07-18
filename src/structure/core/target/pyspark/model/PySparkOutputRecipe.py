from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.core.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.core.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.core.target.pyspark.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.core.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.core.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkOutputRecipe:
    name: str
    ordinal: int
    source: str
    source_scope: str
    input_schema: type[Schema]
    output_schema: type[Schema]
    input_alias: str
    output_alias: str
    filters: tuple[PySparkExpressionRecipe, ...]
    joins: tuple[PySparkJoinRecipe, ...]
    projection: tuple[PySparkProjectionRecipe, ...]
    validation: PySparkValidationRecipe
    operations: tuple[PySparkOperationRecipe, ...] = ()
    aliases: tuple[str, ...] = ()

    @property
    def before_hooks(self) -> tuple[PySparkHookRecipe, ...]:
        return ()

    @property
    def after_hooks(self) -> tuple[PySparkHookRecipe, ...]:
        return ()

    @property
    def validations(self) -> tuple[PySparkValidationRecipe, ...]:
        return (self.validation,)
