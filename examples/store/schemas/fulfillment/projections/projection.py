from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class DemandWindow(Schema):
    """Observed demand grouped into a bounded date interval."""

    tenant = struct(TenantKey, nullable=False)
    product_id = string(nullable=False)
    window_start = date(nullable=False)
    window_end = date(nullable=False)
    requested_quantity = long(nullable=False)
    demand_line_count = long(nullable=False)


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

