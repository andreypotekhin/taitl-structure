from structure import Decimal, Schema, String, field

from orders.schemas.common import AuditStamp, TenantKey


class Promotion(TenantKey, AuditStamp):
    code = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    discount = field(Decimal(12, 2), nullable=True)
