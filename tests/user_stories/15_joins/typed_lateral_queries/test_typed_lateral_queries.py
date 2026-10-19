from typing import cast

from testing.model.orders.schemas.query_operations import ClearanceItem, Inventory, Store

from structure.plugin.pyspark import lateral_join
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.RelationQuery import RelationQuery
from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import PySparkSymbolicContext


def test_a_store_can_select_its_top_clearance_inventory_items() -> None:
    """I can filter, order, and bound a typed relation query for each driving row."""
    store = InputScope(name="store", schema=Store)
    inventory = InputScope(name="inventory", schema=Inventory)

    with PySparkSymbolicContext(step="clearance_items") as context:
        context.register_current_scope("store")
        context.register_relation_scope("inventory", inventory)

        matching = lateral_join(
            inventory,
            where=(cast(Expression, inventory.store_id) == cast(Expression, store.store_id).outer())
            & (cast(Expression, inventory.status) == "Clearance"),
            to=ClearanceItem,
            order_by=(cast(Expression, inventory.quantity).desc(),),
            limit=3,
            how="left",
        )

        join = context.joins[0]
        assert matching._structure_scope_schema is ClearanceItem
        assert cast(RelationQuery, join.query).bound == 3
        assert join.how.value == "left"
        assert join.predicate.kind == "literal"
        assert context.unused_relation_queries() == ()
