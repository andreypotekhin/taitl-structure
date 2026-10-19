from structure import Schema, Transform, input, observation, output, transform
from structure.plugin.pyspark import count, long, observe, string, sum, where


class Order(Schema):
    order_id = string(nullable=False)
    quantity = long(nullable=False)


class OrderMetrics(Schema):
    rows = long(nullable=False)
    total_quantity = long(nullable=True)


@transform
class MeasureAcceptedOrders(Transform):
    orders = input(Order)
    accepted = output(Order)
    quality = observation(OrderMetrics)

    def accept(self, order: Order) -> Order:
        where(order.quantity >= 0)
        observe(OrderMetrics(rows=count(), total_quantity=sum(order.quantity)))
        return Order.project(order)
