from dataclasses import dataclass
from typing import Any

from structure.plugin.pyspark.dsl.joins.AsOf import AsOf
from structure.plugin.pyspark.dsl.joins.TiePolicy import TiePolicy


@dataclass(frozen=True)
class JoinAsOf:
    left_time: Any
    right_time: Any
    direction: AsOf = AsOf.BACKWARD
    tolerance: Any | None = None
    ties: TiePolicy = TiePolicy.ERROR
