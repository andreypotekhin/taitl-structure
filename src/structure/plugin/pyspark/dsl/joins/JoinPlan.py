from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure.plugin.pyspark.dsl.joins.Join import Join
from structure.plugin.pyspark.dsl.joins.JoinAsOf import JoinAsOf
from structure.plugin.pyspark.dsl.joins.JoinDedupe import JoinDedupe
from structure.plugin.pyspark.dsl.joins.JoinHint import JoinHint
from structure.plugin.pyspark.dsl.joins.JoinMethod import JoinMethod
from structure.plugin.pyspark.dsl.joins.JoinStrategy import JoinStrategy
from structure.plugin.pyspark.dsl.joins.JoinTemporal import JoinTemporal


@dataclass(frozen=True)
class JoinPlan:
    input_name: str
    source: str
    input_schema: Any
    predicate: Any
    how: Join
    hint: JoinHint | None = None
    strategy: JoinStrategy | None = None
    method: JoinMethod = JoinMethod.LOOKUP
    dedupe: JoinDedupe | None = None
    temporal: JoinTemporal | None = None
    as_of: JoinAsOf | None = None
    assert_singleton_in_batch: bool = False
