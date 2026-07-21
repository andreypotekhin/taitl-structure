from structure import *
from structure.plugin.pyspark import *

from testing.model.v2.orders.schemas.analytics import CustomerDailyTotal, CustomerEventRank, ProductDailySummary
from testing.model.v2.orders.schemas.order import OrderFulfillment


class OrderAnalytics(Transform):
    fulfilled = input(OrderFulfillment)
    customer_totals = output(CustomerDailyTotal)
    product_summary = output(ProductDailySummary)
    customer_event_rank = output(CustomerEventRank)

    @step(input=fulfilled, output=customer_totals)
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

    @step(input=fulfilled, output=customer_event_rank)
    def customer_event_ranks(self, order: OrderFulfillment) -> CustomerEventRank:
        dedupe_latest_by(order.quantity, partition_by=order.customer_id)
        return CustomerEventRank(
            tenant=order.tenant,
            customer_id=order.customer_id,
            event_id=order.id,
            sequence=order.quantity,
            row_number=row_number(partition_by=order.customer_id, order_by=order.quantity),
            rank=rank(partition_by=order.customer_id, order_by=order.quantity, descending=True),
            dense_rank=dense_rank(partition_by=order.customer_id, order_by=order.quantity),
            previous_sequence=lag(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
            ),
            next_sequence=lead(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
            ),
            rolling_units=rolling_sum(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
                preceding=2,
            ),
            rolling_avg_units=rolling_avg(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
                preceding=2,
            ),
            rolling_min_units=rolling_min(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
                preceding=2,
            ),
            rolling_max_units=rolling_max(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
                preceding=2,
            ),
        )
