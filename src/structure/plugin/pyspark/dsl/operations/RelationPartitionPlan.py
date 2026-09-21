from dataclasses import dataclass

from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class RelationPartitionPlan:
    count: int
    order_by: tuple[Expression, ...]
