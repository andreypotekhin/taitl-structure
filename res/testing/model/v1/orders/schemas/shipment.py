from structure import Schema
from structure.field import *

from testing.model.v1.orders.schemas.common import AuditStamp, TenantKey


class Shipment(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    order_id = string(nullable=False)
    line_number = integer(nullable=False)
    carrier = string(nullable=True)
    tracking_number = string(nullable=True)
    shipped_at = timestamp(nullable=True)
