from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure.platform.pyspark.dsl.joins.Join import Join
from structure.platform.pyspark.dsl.joins.JoinAsOf import JoinAsOf
from structure.platform.pyspark.dsl.joins.JoinDedupe import JoinDedupe
from structure.platform.pyspark.dsl.joins.JoinHint import JoinHint
from structure.platform.pyspark.dsl.joins.JoinMethod import JoinMethod
from structure.platform.pyspark.dsl.joins.JoinStrategy import JoinStrategy
from structure.platform.pyspark.dsl.joins.JoinTemporal import JoinTemporal


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
