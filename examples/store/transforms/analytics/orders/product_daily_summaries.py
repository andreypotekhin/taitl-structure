from examples.store.schemas.analytics import ProductDailySummary
from examples.store.schemas.order import OrderFulfillment
from structure import *
from structure.plugin.pyspark import *


class ProductDailySummaries(Transform):
    fulfilled = input(OrderFulfillment)
    product_summary = output(ProductDailySummary)

    @step(input=fulfilled, output=product_summary)
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
