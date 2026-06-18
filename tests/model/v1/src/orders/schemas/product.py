from structure import Boolean, Decimal, Double, Float, Schema, String, field

from orders.schemas.common import AuditStamp, TenantKey


class Product(TenantKey, AuditStamp):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    category = field(String(), nullable=True)
    active = field(Boolean(), nullable=False)
    list_price = field(Decimal(12, 2), nullable=True)
    weight = field(Float(), nullable=True)
    rating = field(Double(), nullable=True)
