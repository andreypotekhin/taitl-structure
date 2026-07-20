from dataclasses import dataclass

from structure.platform.pyspark.dsl.aggregation.AggregatePlan import AggregatePlan
from structure.platform.pyspark.dsl.aggregation.ProjectAssignment import ProjectAssignment
from structure.platform.pyspark.dsl.Expression import Expression
from structure.platform.pyspark.dsl.joins.JoinPlan import JoinPlan
from structure.platform.pyspark.dsl.operations.OperationPlan import OperationPlan
from structure.platform.pyspark.symbolic_execution.model.PySparkResultBody import PySparkResultBody


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
