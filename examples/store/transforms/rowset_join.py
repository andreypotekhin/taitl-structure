from examples.store.schemas.customer import Customer
from examples.store.schemas.order import (
    CustomerOrderBackfill,
    OrderCustomerReconciliation,
    OrderProductCandidate,
    OrderRaw,
)
from examples.store.schemas.product import Product
from structure import *
from structure.plugin.pyspark import *


class RowsetJoinExamples(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    products = input(Product)
    candidates = output(OrderProductCandidate)

    def reconcile_orders(self, order: OrderRaw, customer: Customer) -> OrderCustomerReconciliation:
        full_join(on=(customer.tenant.tenant_id == order.tenant.tenant_id) & (customer.id == order.customer_id))

        return OrderCustomerReconciliation(
            tenant_id=coalesce(order.tenant.tenant_id, customer.tenant.tenant_id),
            order_id=order.id,
            order_customer_id=order.customer_id,
            customer_id=customer.id,
            customer_name=customer.name,
            match_status=coalesce(customer.tier, "unmatched"),
        )

    def keep_customers(self, row: OrderCustomerReconciliation, customer: Customer) -> CustomerOrderBackfill:
        right_join(on=(customer.tenant.tenant_id == row.tenant_id) & (customer.id == row.customer_id))

        return CustomerOrderBackfill(
            tenant_id=coalesce(row.tenant_id, customer.tenant.tenant_id),
            order_id=row.order_id,
            order_customer_id=row.order_customer_id,
            customer_id=customer.id,
            customer_name=customer.name,
            customer_region=customer.region,
        )

    @step(output=candidates)
    def expand_product_candidates(self, row: CustomerOrderBackfill, product: Product) -> OrderProductCandidate:
        cross_join(product, allow_cartesian=True)

        return OrderProductCandidate(
            tenant_id=row.tenant_id,
            order_id=row.order_id,
            customer_id=row.customer_id,
            customer_name=row.customer_name,
            product_id=product.id,
            product_name=product.name,
        )
