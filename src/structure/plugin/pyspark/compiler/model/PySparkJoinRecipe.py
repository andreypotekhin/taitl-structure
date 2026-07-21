from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinAsOfRecipe import PySparkJoinAsOfRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinDedupeRecipe import PySparkJoinDedupeRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinTemporalRecipe import PySparkJoinTemporalRecipe
from structure.plugin.pyspark.dsl.joins import Join, JoinHint, JoinMethod, JoinStrategy


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
