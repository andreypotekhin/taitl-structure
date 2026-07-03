from examples.orders.schemas.analytics import CustomerDailyTotal, ProductDailySummary
from examples.orders.schemas.order import OrderFulfillment
from structure import Transform, avg, count, count_distinct, group_by, input, max, min, output, sum, transform


@transform
class OrderAnalytics(Transform):
    fulfilled = input(OrderFulfillment)
    customer_totals = output(CustomerDailyTotal)
    product_summary = output(ProductDailySummary)

    @transform(input=fulfilled, output=customer_totals)
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

    @transform(input=fulfilled, output=product_summary)
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
            distinct_customers=count_distinct(order.customer_id),
            units=sum(order.quantity),
            min_units=min(order.quantity),
            max_units=max(order.quantity),
            avg_units=avg(order.quantity),
            gross_total=sum(order.total),
        )
