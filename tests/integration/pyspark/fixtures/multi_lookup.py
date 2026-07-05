from __future__ import annotations

from structure import Join, String, Structure, Transform, after, field, input, join_one, output, transform


class LookupOrder(Structure):
    id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)


class LookupProduct(Structure):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=False)


class LookupEnriched(Structure):
    id = field(String(), nullable=False)
    product_name = field(String(), nullable=True)


@transform
class AddLookupProduct(Transform):
    orders = input(LookupOrder)
    products = input(LookupProduct)
    accepted = output(LookupEnriched)
    audited = output(LookupEnriched)

    @transform(input=[orders, products], output=[accepted, audited])
    def add_product(
        self,
        order: LookupOrder,
        product: LookupProduct,
    ) -> tuple[LookupEnriched, LookupEnriched]:
        product = join_one(
            product,
            on=product.id == order.product_id,
            how=Join.LEFT,
        )
        row = LookupEnriched(id=order.id, product_name=product.name)
        return row, row

    @after(add_product, lane=audited)
    def audit(self, *, audited, spark, ctx):
        return audited
