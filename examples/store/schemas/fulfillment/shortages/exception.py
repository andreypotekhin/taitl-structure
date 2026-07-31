from examples.store.schemas.common import BusinessDate, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class ServiceRiskTarget(Schema):
    tenant = struct(TenantKey, nullable=False)
    target_id = string(nullable=False)
    fill_rate_target = double(nullable=False)
    on_time_target = double(nullable=False)
    active = boolean(nullable=False)


class FulfillmentException(Schema):
    tenant = struct(TenantKey, nullable=False)
    business = struct(BusinessDate, nullable=True)
    order_id = string(nullable=True)
    line_number = integer(nullable=True)
    product_id = string(nullable=True)
    warehouse_id = string(nullable=True)
    target_id = string(nullable=True)
    reason = string(nullable=False)
    severity = integer(nullable=False)
    priority_score = long(nullable=False)
    priority_rank = long(nullable=False)
    due_date = date(nullable=True)
    days_until_due = integer(nullable=True)
    shortage_quantity = long(nullable=False)
    customer_tier = string(nullable=True)
    recommended_action = string(nullable=False)
