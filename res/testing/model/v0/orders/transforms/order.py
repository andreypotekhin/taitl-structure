from structure import Transform, coalesce, expr_fn, input, lower, to_decimal, transform, trim, where

from testing.model.v0.orders.schemas.order import OrderNormalized, OrderRaw


@transform
class NormalizeOrders(Transform):
    orders = input(OrderRaw)

    @expr_fn
    def clean_id(value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())

        return OrderNormalized(
            id=self.clean_id(order.id),
            customer_id=self.clean_id(order.customer_id),
            total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
        )
