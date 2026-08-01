from examples.store.schemas.analytics import CustomerEventRank
from examples.store.schemas.order import OrderFulfillment
from structure import *
from structure.plugin.pyspark import *


class CustomerEventRanks(Transform):
    fulfilled = input(OrderFulfillment)
    customer_event_rank = output(CustomerEventRank)

    @step(input=fulfilled, output=customer_event_rank)
    def customer_event_ranks(self, order: OrderFulfillment) -> CustomerEventRank:
        dedupe_latest_by(order.quantity, partition_by=order.customer_id)
        return CustomerEventRank.project(order)(
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
