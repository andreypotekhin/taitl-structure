from structure import Array, Boolean, Decimal, Integer, Long, Map, String, Struct, Structure, Timestamp, field

from orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey


class OrderRaw(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)
    business = field(Struct(BusinessDate), nullable=False)
    id = field(String(), nullable=False, primary_key=True)
    customer_id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)
    promotion_code = field(String(), nullable=True)
    total = field(String(), nullable=True)
    discount = field(String(), nullable=True)
    quantity = field(Integer(), nullable=True)
    tags = field(Array(String(), contains_null=False), nullable=True)
    attributes = field(Map(String(), String()), nullable=True)
    shipping = field(Struct(Address), nullable=True)


class OrderNormalized(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)
    business = field(Struct(BusinessDate), nullable=False)
    id = field(String(), nullable=False, primary_key=True)
    customer_id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)
    promotion_code = field(String(), nullable=True)
    total = field(Decimal(12, 2), nullable=False)
    discount = field(Decimal(12, 2), nullable=False)
    net_total = field(Decimal(12, 2), nullable=False)
    quantity = field(Long(), nullable=False)
    tags = field(Array(String(), contains_null=False), nullable=True)
    attributes = field(Map(String(), String()), nullable=True)
    shipping = field(Struct(Address), nullable=True)
    is_large = field(Boolean(), nullable=False)


class OrderWithCustomer(OrderNormalized):
    customer_name = field(String(), nullable=True)
    customer_tier = field(String(), nullable=True)
    customer_region = field(String(), nullable=True)


class OrderWithProduct(OrderWithCustomer):
    product_name = field(String(), nullable=True)
    product_category = field(String(), nullable=True)
    product_active = field(Boolean(), nullable=True)
    product_list_price = field(Decimal(12, 2), nullable=True)


class OrderWithPromotion(OrderWithProduct):
    promotion_name = field(String(), nullable=True)
    promotion_discount = field(Decimal(12, 2), nullable=True)


class OrderFulfillment(OrderWithPromotion):
    shipment_line = field(Integer(), nullable=False)
    carrier = field(String(), nullable=True)
    tracking_number = field(String(), nullable=True)
    shipped_at = field(Timestamp(), nullable=True)


class OrderPublication(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    business = field(Struct(BusinessDate), nullable=False)
    id = field(String(), nullable=False, primary_key=True)
    customer_id = field(String(), nullable=False)
    customer_name = field(String(), nullable=True)
    customer_tier = field(String(), nullable=True)
    product_name = field(String(), nullable=True)
    product_category = field(String(), nullable=True)
    promotion_name = field(String(), nullable=True)
    total = field(Decimal(12, 2), nullable=False)
    discount = field(Decimal(12, 2), nullable=False)
    net_total = field(Decimal(12, 2), nullable=False)
    quantity = field(Long(), nullable=False)
    carrier = field(String(), nullable=True)
    tracking_number = field(String(), nullable=True)
    shipped_at = field(Timestamp(), nullable=True)
    is_large = field(Boolean(), nullable=False)


class PublicationFlags(Structure):
    has_promotion = field(Boolean(), nullable=False)


class OrderPublished(OrderPublication, PublicationFlags):
    pass
