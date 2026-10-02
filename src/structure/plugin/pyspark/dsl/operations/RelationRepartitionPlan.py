from dataclasses import dataclass

from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class RelationRepartitionPlan:
    partitions: int | None
    keys: tuple[Expression, ...]
