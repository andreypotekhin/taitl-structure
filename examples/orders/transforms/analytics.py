import structure
from examples.orders.schemas.analytics import CustomerDailyTotal, CustomerEventRank, ProductDailySummary
from examples.orders.schemas.order import OrderFulfillment


class OrderAnalytics(structure.Transform):
    fulfilled = structure.input(OrderFulfillment)
    customer_totals = structure.output(CustomerDailyTotal)
    product_summary = structure.output(ProductDailySummary)
    customer_event_rank = structure.output(CustomerEventRank)

    @structure.step(input=fulfilled, output=customer_totals)
    def customer_daily_totals(self, order: OrderFulfillment) -> CustomerDailyTotal:
        structure.group_by(
            tenant_id=order.tenant.tenant_id,
            customer_id=order.customer_id,
            order_date=order.business.order_date,
        )

        return CustomerDailyTotal(
            tenant=order.tenant,
            customer_id=order.customer_id,
            order_date=order.business.order_date,
            order_count=structure.count(),
            gross_total=structure.sum(order.total),
            net_total=structure.sum(order.net_total),
        )

    @structure.step(input=fulfilled, output=product_summary)
    def product_daily_summary(self, order: OrderFulfillment) -> ProductDailySummary:
        structure.group_by(
            tenant_id=order.tenant.tenant_id,
            product_id=order.product_id,
            order_date=order.business.order_date,
        )

        return ProductDailySummary(
            tenant=order.tenant,
            product_id=order.product_id,
            order_date=order.business.order_date,
            order_count=structure.count(),
            distinct_customers=structure.count_distinct(order.customer_id),
            units=structure.sum(order.quantity),
            min_units=structure.min(order.quantity),
            max_units=structure.max(order.quantity),
            avg_units=structure.avg(order.quantity),
            gross_total=structure.sum(order.total),
        )

    @structure.step(input=fulfilled, output=customer_event_rank)
    def customer_event_ranks(self, order: OrderFulfillment) -> CustomerEventRank:
        structure.dedupe_latest_by(order.quantity, partition_by=order.customer_id)
        return CustomerEventRank(
            tenant=order.tenant,
            customer_id=order.customer_id,
            event_id=order.id,
            sequence=order.quantity,
            row_number=structure.row_number(partition_by=order.customer_id, order_by=order.quantity),
            rank=structure.rank(partition_by=order.customer_id, order_by=order.quantity, descending=True),
            dense_rank=structure.dense_rank(partition_by=order.customer_id, order_by=order.quantity),
            previous_sequence=structure.lag(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
            ),
            next_sequence=structure.lead(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
            ),
            rolling_units=structure.rolling_sum(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
                preceding=2,
            ),
            rolling_avg_units=structure.rolling_avg(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
                preceding=2,
            ),
            rolling_min_units=structure.rolling_min(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
                preceding=2,
            ),
            rolling_max_units=structure.rolling_max(
                order.quantity,
                partition_by=order.customer_id,
                order_by=order.quantity,
                preceding=2,
            ),
        )
