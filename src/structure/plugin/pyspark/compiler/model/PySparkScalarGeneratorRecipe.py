from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkScalarGeneratorRecipe:
    expression: PySparkExpressionRecipe
    scope: str
    schema: type[Schema]
    value_field: str
    ordinal: str | None
    function: str = "posexplode"
    outer: bool = False
