from orders.schemas import OrderPublished, OrderRaw

from structure import Transform, coalesce, input, output, to_decimal, transform, where


@transform
class PublishOrders(Transform):
    orders = input(OrderRaw)
    published = output(OrderPublished)

    def publish(self, order: OrderRaw) -> OrderPublished:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())

        return OrderPublished(
            id=order.id,
            customer_id=order.customer_id,
            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
            status="ready",
        )
