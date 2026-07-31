from examples.store.schemas.fulfillment.planning.plan import FulfillmentPlan
from examples.store.schemas.fulfillment.reconciliation.reconciliation import FulfillmentReconciliation
from examples.store.schemas.order import OrderFulfillment
from structure import *
from structure.plugin.pyspark import *


class ReconcileFulfillmentPlan(Transform):
    plans = input(FulfillmentPlan)
    fulfilled = input(OrderFulfillment)
    reconciliation = output(FulfillmentReconciliation)

    @step(output=reconciliation)
    def reconcile(self, plan: FulfillmentPlan, fulfilled: OrderFulfillment) -> FulfillmentReconciliation:
        left_join(
            on=(fulfilled.tenant.tenant_id == plan.tenant.tenant_id)
            & (fulfilled.id == plan.order_id)
            & (fulfilled.product_id == plan.product_id)
        )
        group_by(
            tenant_id=plan.tenant.tenant_id,
            order_id=plan.order_id,
            product_id=plan.product_id,
            business=plan.business,
            planned_status=plan.plan_status,
            allocated_quantity=plan.allocated_quantity,
            backordered_quantity=plan.backordered_quantity,
        )
        shipped_quantity = sum(coalesce(fulfilled.quantity, 0))
        return FulfillmentReconciliation.project(plan)(
            planned_status=plan.plan_status,
            planned_allocated_quantity=plan.allocated_quantity,
            planned_backordered_quantity=plan.backordered_quantity,
            shipped_quantity=shipped_quantity,
            shipped=bool_or(fulfilled.id.is_not_null()),
            reconciliation_status=min(
                when(fulfilled.id.is_not_null(), "planned_line_shipped").otherwise("planned_line_not_shipped")
            ),
        )
