from structure import Integer, Schema, String, Struct, Timestamp, field

from orders.schemas.common import AuditStamp, TenantKey


class Shipment(Schema):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)
    order_id = field(String(), nullable=False)
    line_number = field(Integer(), nullable=False)
    carrier = field(String(), nullable=True)
    tracking_number = field(String(), nullable=True)
    shipped_at = field(Timestamp(), nullable=True)
