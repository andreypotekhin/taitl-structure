from dataclasses import dataclass

from structure.plugin.pyspark.dsl.aggregation.AggregatePlan import AggregatePlan
from structure.plugin.pyspark.dsl.aggregation.ProjectAssignment import ProjectAssignment


@dataclass(frozen=True)
class PySparkResultBody:
    """PySpark-only payload for one Core-routed step result."""

    projection: tuple[ProjectAssignment, ...] = ()
    aggregate: AggregatePlan | None = None
