from examples.store.schemas.common import BusinessDate, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class FulfillmentAllocation(Schema):
    tenant = struct(TenantKey, nullable=False)
    business = struct(BusinessDate, nullable=False)
    order_id = string(nullable=False)
    line_number = integer(nullable=False)
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    warehouse_id = string(nullable=False)
    allocated_quantity = long(nullable=False)
    requested_quantity = long(nullable=False)
    planned_ship_date = date(nullable=True)


class FulfillmentBackorder(Schema):
    tenant = struct(TenantKey, nullable=False)
    business = struct(BusinessDate, nullable=False)
    order_id = string(nullable=False)
    line_number = integer(nullable=False)
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    warehouse_id = string(nullable=True)
    backordered_quantity = long(nullable=False)
    requested_quantity = long(nullable=False)
    planned_ship_date = date(nullable=True)
    reason = string(nullable=False)


class FulfillmentPlan(Schema):
    tenant = struct(TenantKey, nullable=False)
    business = struct(BusinessDate, nullable=False)
    order_id = string(nullable=False)
    line_number = integer(nullable=False)
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    requested_quantity = long(nullable=False)
    allocated_quantity = long(nullable=False)
    backordered_quantity = long(nullable=False)
    selected_warehouse_id = string(nullable=True)
    planned_ship_date = date(nullable=True)
    is_fully_allocated = boolean(nullable=False)
    plan_status = string(nullable=False)


class ReplenishmentSuggestion(Schema):
    tenant = struct(TenantKey, nullable=False)
    warehouse_id = string(nullable=False)
    product_id = string(nullable=False)
    available_to_promise_after_plan = long(nullable=False)
    safety_stock_quantity = long(nullable=False)
    earliest_inbound_at = date(nullable=True)
    reason = string(nullable=False)
