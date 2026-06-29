from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.app.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkOutputRecipe:
    name: str
    ordinal: int
    source: str
    source_scope: str
    input_schema: type[Structure]
    output_schema: type[Structure]
    input_alias: str
    output_alias: str
    filters: tuple[PySparkExpressionRecipe, ...]
    joins: tuple[PySparkJoinRecipe, ...]
    projection: tuple[PySparkProjectionRecipe, ...]
    validation: PySparkValidationRecipe
    operations: tuple[PySparkOperationRecipe, ...] = ()

    @property
    def before_hooks(self) -> tuple[PySparkHookRecipe, ...]:
        return ()

    @property
    def after_hooks(self) -> tuple[PySparkHookRecipe, ...]:
        return ()

    @property
    def validations(self) -> tuple[PySparkValidationRecipe, ...]:
        return (self.validation,)
