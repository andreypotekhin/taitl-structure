from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.platform.pyspark.compiler.model.PySparkJoinAsOfRecipe import PySparkJoinAsOfRecipe
from structure.platform.pyspark.compiler.model.PySparkJoinDedupeRecipe import PySparkJoinDedupeRecipe
from structure.platform.pyspark.compiler.model.PySparkJoinTemporalRecipe import PySparkJoinTemporalRecipe
from structure.platform.pyspark.dsl.joins import Join, JoinHint, JoinMethod, JoinStrategy


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
