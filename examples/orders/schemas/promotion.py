from examples.orders.schemas.common import AuditStamp, TenantKey
from structure import Decimal, String, Struct, Structure, field


class Promotion(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)
    code = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    discount = field(Decimal(12, 2), nullable=True)
