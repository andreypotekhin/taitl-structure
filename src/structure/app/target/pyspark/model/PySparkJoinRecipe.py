from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.Join import Join
from structure.app.dsl.model.transforms.JoinHint import JoinHint
from structure.app.dsl.model.transforms.JoinStrategy import JoinStrategy
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkJoinDedupeRecipe import PySparkJoinDedupeRecipe


@dataclass(frozen=True)
class PySparkJoinRecipe:
    input_name: str
    source: str
    input_schema: type[Structure]
    left_alias: str
    right_alias: str
    how: Join
    hint: JoinHint | None
    predicate: PySparkExpressionRecipe
    occurrence: int
    method: JoinMethod = JoinMethod.ONE
    strategy: JoinStrategy | None = None
    dedupe: PySparkJoinDedupeRecipe | None = None
