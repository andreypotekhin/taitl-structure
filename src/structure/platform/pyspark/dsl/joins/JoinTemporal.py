from dataclasses import dataclass
from typing import Any

from structure.platform.pyspark.dsl.joins.OverlapPolicy import OverlapPolicy


@dataclass(frozen=True)
class JoinTemporal:
    at: Any
    valid_from: Any
    valid_to: Any
    overlaps: OverlapPolicy = OverlapPolicy.ERROR
