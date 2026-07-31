from examples.store.schemas.customer import Customer
from examples.store.schemas.order import CustomerOrderBackfill, OrderCustomerReconciliation
from structure import *
from structure.plugin.pyspark import *


class BackfillCustomers(Transform):
    reconciliation = input(OrderCustomerReconciliation)
    customers = input(Customer)
    backfills = output(CustomerOrderBackfill)

    @step(input=[reconciliation, customers], output=backfills)
    def keep_customers(self, row: OrderCustomerReconciliation, customer: Customer) -> CustomerOrderBackfill:
        right_join(on=(customer.tenant.tenant_id == row.tenant_id) & (customer.id == row.customer_id))
        return CustomerOrderBackfill.project(customer, row)(
            tenant_id=coalesce(row.tenant_id, customer.tenant.tenant_id),
            customer_id=customer.id,
            customer_name=customer.name,
            customer_region=customer.region,
        )
