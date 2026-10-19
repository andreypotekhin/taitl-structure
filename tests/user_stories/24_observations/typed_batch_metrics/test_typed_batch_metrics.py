from importlib import import_module

from structure.plugin.pyspark import count, observe, sum
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import PySparkSymbolicContext

models = import_module("tests.user_stories.24_observations.typed_batch_metrics.models")
MeasureAcceptedOrders = models.MeasureAcceptedOrders
Order = models.Order
OrderMetrics = models.OrderMetrics


def test_named_observation_schema_binds_typed_metrics():
    context = PySparkSymbolicContext(step="accept")
    context.observation_declarations = tuple(MeasureAcceptedOrders._structure_observations.values())
    context.default_project_source = "accepted"
    order = RowScope(name="order", schema=Order)
    with context:
        observed = observe(OrderMetrics(rows=count(), total_quantity=sum(order.quantity)))

    assert observed == "accepted"
    plan = context.operations[0].observation
    assert plan is not None
    assert plan.name == "quality"
    assert plan.schema is OrderMetrics
