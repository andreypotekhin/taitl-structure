from __future__ import annotations

from structure import *
from structure.platform.pyspark import *


class LookupOrder(Schema):
    id = string(nullable=False)
    product_id = string(nullable=False)


class LookupProduct(Schema):
    id = string(nullable=False)
    name = string(nullable=False)


class LookupEnriched(Schema):
    id = string(nullable=False)
    product_name = string(nullable=True)


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
