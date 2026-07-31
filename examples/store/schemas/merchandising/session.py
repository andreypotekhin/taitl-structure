from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class SessionEvent(Schema):
    tenant = struct(TenantKey, nullable=False)
    id = string(nullable=False)
    session_id = string(nullable=False)
    customer_id = string(nullable=True)
    event_type = string(nullable=False)
    product_id = string(nullable=True)
    category = string(nullable=True)
    occurred_at = timestamp(nullable=False)


class SessionFeature(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    session_id = string(nullable=False)
    customer_id = string(nullable=True)
    product_id = string(nullable=True)
    category = string(nullable=True)
    event_count = long(nullable=False)
    product_view_count = long(nullable=False)
    add_to_cart_count = long(nullable=False)
    last_event_at = timestamp(nullable=False)
