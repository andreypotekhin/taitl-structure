from structure import Decimal, Schema, String, Struct, field

from orders.schemas.common import AuditStamp, TenantKey


class Promotion(Schema):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)
    code = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    discount = field(Decimal(12, 2), nullable=True)
