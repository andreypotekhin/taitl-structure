from examples.store.schemas.fulfillment.demand import Order
from examples.store.schemas.fulfillment.projections import DemandWindow
from structure import *
from structure.plugin.pyspark import *


class BuildDemandWindows(Transform):
    """Turn observed order demand into deterministic daily demand windows."""

    demand = input(Order)
    windows = output(DemandWindow)

    @step(output=windows)
    def build(self, order: Order) -> DemandWindow:
        where(order.business.order_date.is_not_null())
        group_by(
            tenant_id=order.tenant.tenant_id,
            product_id=order.product_id,
            window_start=order.business.order_date,
            window_end=order.business.order_date,
        )
        return DemandWindow(
            tenant=order.tenant,
            product_id=order.product_id,
            window_start=order.business.order_date,
            window_end=order.business.order_date,
            requested_quantity=sum(order.requested_quantity),
            demand_line_count=count(),
        )
