from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class DailyFulfillmentSummary(Schema):
    tenant = struct(TenantKey, nullable=False)
    business_date = date(nullable=True)
    demand_line_count = long(nullable=False)
    allocated_line_count = long(nullable=False)
    partially_allocated_line_count = long(nullable=False)
    backordered_line_count = long(nullable=False)
    requested_units = long(nullable=False)
    allocated_units = long(nullable=False)
    backordered_units = long(nullable=False)
    fill_rate = double(nullable=True)
    backorder_rate = double(nullable=True)


class WarehouseLoadSummary(Schema):
    tenant = struct(TenantKey, nullable=False)
    warehouse_id = string(nullable=False)
    business_date = date(nullable=True)
    planned_line_count = long(nullable=False)
    allocated_units = long(nullable=False)
    distinct_products = long(nullable=False)
    customer_count = long(nullable=False)
