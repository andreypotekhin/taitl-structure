from examples.orders.schemas.common import AuditStamp, TenantKey
from structure import Integer, String, Struct, Structure, Timestamp, field


class Shipment(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)
    order_id = field(String(), nullable=False)
    line_number = field(Integer(), nullable=False)
    carrier = field(String(), nullable=True)
    tracking_number = field(String(), nullable=True)
    shipped_at = field(Timestamp(), nullable=True)
