from examples.store.schemas.customer import Customer
from examples.store.schemas.order import OrderProductCandidate, OrderRaw
from examples.store.schemas.product import Product
from examples.store.transforms.rowset_joins.backfill_customers import BackfillCustomers
from examples.store.transforms.rowset_joins.expand_customer_products import ExpandCustomerProducts
from examples.store.transforms.rowset_joins.reconcile_orders_and_customers import ReconcileOrdersAndCustomers
from structure import Transform, input, output


class RowsetJoinExamples(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    products = input(Product)

    reconciled = ReconcileOrdersAndCustomers(orders=orders, customers=customers)
    backfilled = BackfillCustomers(reconciliation=reconciled.reconciliation, customers=customers)
    expanded = ExpandCustomerProducts(backfills=backfilled.backfills, products=products)

    candidates = output(OrderProductCandidate)
    result = output(candidates=expanded.candidates)
