from examples.store.schemas.analytics import CustomerDailyTotal
from examples.store.schemas.order import OrderFulfillment
from structure import *
from structure.plugin.pyspark import *


class CustomerDailyTotals(Transform):
    fulfilled = input(OrderFulfillment)
    customer_totals = output(CustomerDailyTotal)

    @step(input=fulfilled, output=customer_totals)
    def customer_daily_totals(self, order: OrderFulfillment) -> CustomerDailyTotal:
        group_by(
            tenant_id=order.tenant.tenant_id,
            customer_id=order.customer_id,
            order_date=order.business.order_date,
        )
        return CustomerDailyTotal.project(order)(
            order_date=order.business.order_date,
            order_count=count(),
            gross_total=sum(order.total),
            net_total=sum(order.net_total),
        )
