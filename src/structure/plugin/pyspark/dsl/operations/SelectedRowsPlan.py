from dataclasses import dataclass
from typing import Any

from structure.plugin.pyspark.dsl.joins import TiePolicy


@dataclass(frozen=True)
class SelectedRowsPlan:
    direction: str
    order_by: Any
    partition_by: tuple[Any, ...]
    ties: TiePolicy
