from structure import Integer, Schema, String, Timestamp, field

from orders.schemas.common import AuditStamp, TenantKey


class Shipment(TenantKey, AuditStamp):
    order_id = field(String(), nullable=False)
    line_number = field(Integer(), nullable=False)
    carrier = field(String(), nullable=True)
    tracking_number = field(String(), nullable=True)
    shipped_at = field(Timestamp(), nullable=True)
