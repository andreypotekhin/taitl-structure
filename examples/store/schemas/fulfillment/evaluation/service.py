from examples.store.schemas.common import BusinessDate, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class FulfillmentServiceTotals(Schema):
    tenant = struct(TenantKey, nullable=False)
    business = struct(BusinessDate, nullable=False)
    order_id = string(nullable=False)
    line_number = integer(nullable=False)
    product_id = string(nullable=False)
    selected_warehouse_id = string(nullable=True)
    target_id = string(nullable=True)
    target_on_time = double(nullable=True)
    requested_quantity = long(nullable=False)
    planned_quantity = long(nullable=False)
    shipped_quantity = long(nullable=False)
    planned_ship_date = date(nullable=True)
    actual_ship_date = date(nullable=True)
    shipped = boolean(nullable=False)


class FulfillmentServiceEvaluation(Schema):
    tenant = struct(TenantKey, nullable=False)
    business = struct(BusinessDate, nullable=False)
    order_id = string(nullable=False)
    line_number = integer(nullable=False)
    product_id = string(nullable=False)
    selected_warehouse_id = string(nullable=True)
    target_id = string(nullable=True)
    target_on_time = double(nullable=True)
    requested_quantity = long(nullable=False)
    planned_quantity = long(nullable=False)
    shipped_quantity = long(nullable=False)
    planned_ship_date = date(nullable=True)
    actual_ship_date = date(nullable=True)
    on_time_status = string(nullable=False)
    in_full_status = string(nullable=False)
    lateness_days = integer(nullable=True)
    service_status = string(nullable=False)


class DailyFulfillmentServiceSummary(Schema):
    tenant = struct(TenantKey, nullable=False)
    business_date = date(nullable=True)
    warehouse_id = string(nullable=True)
    target_id = string(nullable=True)
    evaluated_line_count = long(nullable=False)
    on_time_in_full_count = long(nullable=False)
    on_time_line_count = long(nullable=False)
    in_full_line_count = long(nullable=False)
    service_level = double(nullable=True)
    target_on_time = double(nullable=True)
    target_attained = boolean(nullable=True)
