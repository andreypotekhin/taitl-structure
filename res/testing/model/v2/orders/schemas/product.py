from structure import Boolean, Decimal, Double, Float, String, Struct, Structure, field

from testing.model.v2.orders.schemas.common import AuditStamp, TenantKey


class ProductBase(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)


class Product(ProductBase):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    category = field(String(), nullable=True)
    active = field(Boolean(), nullable=False)
    list_price = field(Decimal(12, 2), nullable=True)
    weight = field(Float(), nullable=True)
    rating = field(Double(), nullable=True)


class BlockedProduct(ProductBase):
    product_id = field(String(), nullable=False, primary_key=True)
    reason = field(String(), nullable=True)
