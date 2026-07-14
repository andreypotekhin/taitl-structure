from __future__ import annotations

from structure import *


class LookupOrder(Schema):
    id = field.string(nullable=False)
    product_id = field.string(nullable=False)


class LookupProduct(Schema):
    id = field.string(nullable=False)
    name = field.string(nullable=False)


class LookupEnriched(Schema):
    id = field.string(nullable=False)
    product_name = field.string(nullable=True)


@transform
class AddLookupProduct(Transform):
    orders = input(LookupOrder)
    products = input(LookupProduct)
    accepted = output(LookupEnriched)
    audited = output(LookupEnriched)

    @step(input=[orders, products], output=[accepted, audited])
    def add_product(
        self,
        order: LookupOrder,
        product: LookupProduct,
    ) -> tuple[LookupEnriched, LookupEnriched]:
        product = lookup_join(
            product,
            on=product.id == order.product_id,
            how=Join.LEFT,
        )
        row = LookupEnriched(id=order.id, product_name=product.name)
        return row, row

    @raw(inout=lane(audited) | output(audited))
    def audit(self, *, audited, spark, ctx):
        return audited
