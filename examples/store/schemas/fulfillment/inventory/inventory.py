from examples.store.schemas.common import AuditStamp, TenantKey
from examples.store.schemas.fulfillment.planning.inventory import InboundInventory, InventoryPosition, Warehouse
from structure import Schema
from structure.plugin.pyspark import *


class LeadTime(Schema):
    """Declared calendar days from a supply decision to usable inventory."""

    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    warehouse_id = string(nullable=False)
    product_id = string(nullable=False)
    days = integer(nullable=False)
    active = boolean(nullable=False)


__all__ = ["InboundInventory", "InventoryPosition", "LeadTime", "Warehouse"]
