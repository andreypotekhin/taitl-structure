from examples.store.schemas.customer import Customer
from examples.store.schemas.order import OrderCustomerReconciliation, OrderRaw
from structure import *
from structure.plugin.pyspark import *


class ReconcileOrdersAndCustomers(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    reconciliation = output(OrderCustomerReconciliation)

    @step(input=[orders, customers], output=reconciliation)
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
