from structure import Transform, count, group_by, input, sum, transform

from testing.model.v2.orders.schemas.analytics import CustomerDailyTotal, ProductDailySummary
from testing.model.v2.orders.schemas.order import OrderFulfillment


@transform
class OrderAnalytics(Transform):
    fulfilled = input(OrderFulfillment)

    def customer_daily_totals(self, order: OrderFulfillment) -> CustomerDailyTotal:
        group_by(
            tenant_id=order.tenant.tenant_id,
            customer_id=order.customer_id,
            order_date=order.business.order_date,
        )

        return CustomerDailyTotal(
            tenant=order.tenant,
            customer_id=order.customer_id,
            order_date=order.business.order_date,
            order_count=count(),
            gross_total=sum(order.total),
            net_total=sum(order.net_total),
        )

    def product_daily_summary(self, order: OrderFulfillment) -> ProductDailySummary:
        group_by(
            tenant_id=order.tenant.tenant_id,
            product_id=order.product_id,
            order_date=order.business.order_date,
        )

        return ProductDailySummary(
            tenant=order.tenant,
            product_id=order.product_id,
            order_date=order.business.order_date,
            order_count=count(),
            units=sum(order.quantity),
            gross_total=sum(order.total),
        )
