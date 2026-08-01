from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class InventoryProjection(Schema):
    """Dated inventory arithmetic retained with the facts that produced it."""

    tenant = struct(TenantKey, nullable=False)
    warehouse_id = string(nullable=False)
    product_id = string(nullable=False)
    window_start = date(nullable=False)
    window_end = date(nullable=False)
    usable_at = date(nullable=False)
    opening_quantity = long(nullable=False)
    inbound_quantity = long(nullable=False)
    demand_quantity = long(nullable=False)
    reserved_quantity = long(nullable=False)
    projected_available_quantity = long(nullable=False)
    safety_stock_quantity = long(nullable=False)
