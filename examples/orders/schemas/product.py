from examples.orders.schemas.common import AuditStamp, TenantKey
from structure import Boolean, Decimal, Double, Float, String, Struct, Structure, field


class Product(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    category = field(String(), nullable=True)
    active = field(Boolean(), nullable=False)
    list_price = field(Decimal(12, 2), nullable=True)
    weight = field(Float(), nullable=True)
    rating = field(Double(), nullable=True)
