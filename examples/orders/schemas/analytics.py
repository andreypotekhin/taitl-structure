from examples.orders.schemas.common import TenantKey
from structure import Date, Decimal, Double, Long, String, Struct, Structure, field


class CustomerDailyTotal(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    customer_id = field(String(), nullable=False)
    order_date = field(Date(), nullable=True)
    order_count = field(Long(), nullable=False)
    gross_total = field(Decimal(12, 2), nullable=False)
    net_total = field(Decimal(12, 2), nullable=False)


class ProductDailySummary(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    product_id = field(String(), nullable=False)
    order_date = field(Date(), nullable=True)
    order_count = field(Long(), nullable=False)
    distinct_customers = field(Long(), nullable=False)
    units = field(Long(), nullable=False)
    min_units = field(Long(), nullable=False)
    max_units = field(Long(), nullable=False)
    avg_units = field(Double(), nullable=False)
    gross_total = field(Decimal(12, 2), nullable=False)
