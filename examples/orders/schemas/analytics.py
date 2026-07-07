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


class CustomerEventRank(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    customer_id = field(String(), nullable=False)
    event_id = field(String(), nullable=False)
    sequence = field(Long(), nullable=False)
    row_number = field(Long(), nullable=False)
    rank = field(Long(), nullable=False)
    dense_rank = field(Long(), nullable=False)
    previous_sequence = field(Long(), nullable=True)
    next_sequence = field(Long(), nullable=True)
    rolling_units = field(Long(), nullable=False)
    rolling_avg_units = field(Double(), nullable=False)
    rolling_min_units = field(Long(), nullable=False)
    rolling_max_units = field(Long(), nullable=False)
