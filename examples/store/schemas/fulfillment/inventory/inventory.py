from examples.store.schemas.common import AuditStamp, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class InventoryPosition(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    warehouse_id = string(nullable=False)
    product_id = string(nullable=False)
    on_hand_quantity = long(nullable=False)
    reserved_quantity = long(nullable=False)
    safety_stock_quantity = long(nullable=False)
    as_of = date(nullable=False)


class InboundInventory(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    warehouse_id = string(nullable=False)
    product_id = string(nullable=False)
    expected_quantity = long(nullable=False)
    expected_at = date(nullable=True)
    source_type = string(nullable=False)


class LeadTime(Schema):
    """Declared calendar days from a supply decision to usable inventory."""

    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    warehouse_id = string(nullable=False)
    product_id = string(nullable=False)
    days = integer(nullable=False)
    active = boolean(nullable=False)


__all__ = ["InboundInventory", "InventoryPosition", "LeadTime"]
