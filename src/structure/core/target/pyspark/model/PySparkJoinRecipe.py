from __future__ import annotations

from dataclasses import dataclass

from structure.core.compiler.ir.model.JoinMethod import JoinMethod
from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.Join import Join
from structure.core.dsl.model.transforms.JoinHint import JoinHint
from structure.core.dsl.model.transforms.JoinStrategy import JoinStrategy
from structure.core.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.core.target.pyspark.model.PySparkJoinAsOfRecipe import PySparkJoinAsOfRecipe
from structure.core.target.pyspark.model.PySparkJoinDedupeRecipe import PySparkJoinDedupeRecipe
from structure.core.target.pyspark.model.PySparkJoinTemporalRecipe import PySparkJoinTemporalRecipe


@dataclass(frozen=True)
class PySparkJoinRecipe:
    input_name: str
    source: str
    input_schema: type[Schema]
    left_alias: str
    right_alias: str
    how: Join
    hint: JoinHint | None
    predicate: PySparkExpressionRecipe
    occurrence: int
    method: JoinMethod = JoinMethod.LOOKUP
    strategy: JoinStrategy | None = None
    dedupe: PySparkJoinDedupeRecipe | None = None
    temporal: PySparkJoinTemporalRecipe | None = None
    as_of: PySparkJoinAsOfRecipe | None = None
