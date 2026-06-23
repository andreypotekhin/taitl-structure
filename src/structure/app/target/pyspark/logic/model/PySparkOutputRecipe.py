from __future__ import annotations

from dataclasses import dataclass

from structure.app.target.pyspark.logic.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.logic.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.logic.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.logic.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.target.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe
from structure.app.dsl.logic.model.schemas.Structure import Structure


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

    @property
    def before_hooks(self) -> tuple[PySparkHookRecipe, ...]:
        return ()

    @property
    def after_hooks(self) -> tuple[PySparkHookRecipe, ...]:
        return ()

    @property
    def validations(self) -> tuple[PySparkValidationRecipe, ...]:
        return (self.validation,)
