from structure import Schema
from structure.plugin.pyspark import *

from testing.model.v2.orders.schemas.common import TenantKey


class CustomerDailyTotal(Schema):
    tenant = struct(TenantKey, nullable=False)
    customer_id = string(nullable=False)
    order_date = date(nullable=True)
    order_count = long(nullable=False)
    gross_total = decimal(22, 2, nullable=False)
    net_total = decimal(22, 2, nullable=False)


class ProductDailySummary(Schema):
    tenant = struct(TenantKey, nullable=False)
    product_id = string(nullable=False)
    order_date = date(nullable=True)
    order_count = long(nullable=False)
    distinct_customers = long(nullable=False)
    units = long(nullable=False)
    min_units = long(nullable=False)
    max_units = long(nullable=False)
    avg_units = double(nullable=False)
    gross_total = decimal(22, 2, nullable=False)


class CustomerEventRank(Schema):
    tenant = struct(TenantKey, nullable=False)
    customer_id = string(nullable=False)
    event_id = string(nullable=False)
    sequence = long(nullable=False)
    row_number = long(nullable=False)
    rank = long(nullable=False)
    dense_rank = long(nullable=False)
    previous_sequence = long(nullable=True)
    next_sequence = long(nullable=True)
    rolling_units = long(nullable=False)
    rolling_avg_units = double(nullable=False)
    rolling_min_units = long(nullable=False)
    rolling_max_units = long(nullable=False)
