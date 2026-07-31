from examples.store.schemas.order import CustomerOrderBackfill, OrderProductCandidate
from examples.store.schemas.product import Product
from structure import *
from structure.plugin.pyspark import *


class ExpandCustomerProducts(Transform):
    backfills = input(CustomerOrderBackfill)
    products = input(Product)
    candidates = output(OrderProductCandidate)

    @step(input=[backfills, products], output=candidates)
    def expand_product_candidates(self, row: CustomerOrderBackfill, product: Product) -> OrderProductCandidate:
        cross_join(product, allow_cartesian=True)
        return OrderProductCandidate.project(row, product)(
            product_id=product.id,
            product_name=product.name,
        )
