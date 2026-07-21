from dataclasses import dataclass

from structure.plugin.pyspark.dsl.aggregation.AggregatePlan import AggregatePlan
from structure.plugin.pyspark.dsl.aggregation.ProjectAssignment import ProjectAssignment
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.joins.JoinPlan import JoinPlan
from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkResultBody import PySparkResultBody


@dataclass(frozen=True)
class PySparkStepBody:
    """The PySpark symbolic state captured while Core invokes one step method."""

    value: object
    filters: tuple[Expression, ...] = ()
    joins: tuple[JoinPlan, ...] = ()
    operations: tuple[OperationPlan, ...] = ()
    aggregate_keys: tuple[tuple[str, Expression], ...] | None = None
    aggregate_levels: tuple[tuple[str, ...], ...] = ()
    aggregate_grouping: str = "group_by"
    aggregate_having: Expression | None = None
    projection: tuple[ProjectAssignment, ...] = ()
    aggregate: AggregatePlan | None = None
    results: tuple[PySparkResultBody, ...] = ()
