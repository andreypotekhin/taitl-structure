from structure import Schema, Transform, input, output
from structure.plugin.pyspark import field
from orders.schemas.order import OrderPublished, OrderRaw


class OrderNormalized(Schema):
    id = field.string(nullable=False)


class PublishOrders(Transform):
    orders = input(OrderRaw)
    published = output(OrderPublished)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized(id=order.id)

    def publish(self, order: OrderNormalized) -> OrderPublished:
        return OrderPublished(id=order.id)
