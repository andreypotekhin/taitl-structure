from examples.store.schemas.common import BusinessDate, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class FulfillmentReconciliation(Schema):
    tenant = struct(TenantKey, nullable=False)
    business = struct(BusinessDate, nullable=False)
    order_id = string(nullable=False)
    line_number = integer(nullable=False)
    product_id = string(nullable=False)
    planned_status = string(nullable=False)
    planned_allocated_quantity = long(nullable=False)
    planned_backordered_quantity = long(nullable=False)
    shipped_quantity = long(nullable=False)
    shipped = boolean(nullable=False)
    reconciliation_status = string(nullable=False)


# Compatibility name for callers using the original plan-versus-actual name.
PlannedActualReconciliation = FulfillmentReconciliation
