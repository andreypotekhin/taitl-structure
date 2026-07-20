from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.platform.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.platform.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.platform.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.platform.pyspark.compiler.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.platform.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


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
