from examples.store.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class OrderRaw(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    business = struct(BusinessDate, nullable=False)
    id = string(nullable=False)
    line_number = integer(nullable=False)
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    promotion_code = string(nullable=True, alias='promo-code')
    total = string(nullable=True)
    discount = string(nullable=True)
    quantity = integer(nullable=True)
    tags = array(string(), contains_null=False, nullable=True)
    attributes = map(string(), string(), nullable=True)
    shipping = struct(Address, nullable=True)


class OrderNormalized(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    business = struct(BusinessDate, nullable=False)
    id = string(nullable=False)
    line_number = integer(nullable=False)
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    promotion_code = string(nullable=True)
    total = decimal(12, 2, nullable=False)
    discount = decimal(12, 2, nullable=False)
    net_total = decimal(12, 2, nullable=False)
    quantity = long(nullable=False)
    tags = array(string(), contains_null=False, nullable=True)
    attributes = map(string(), string(), nullable=True)
    shipping = struct(Address, nullable=True)
    is_large = boolean(nullable=False)


class OrderWithCustomer(OrderNormalized):
    customer_name = string(nullable=True)
    customer_tier = string(nullable=True)
    customer_region = string(nullable=True)


class OrderWithProduct(OrderWithCustomer):
    product_name = string(nullable=True)
    product_category = string(nullable=True)
    product_active = boolean(nullable=True)
    product_list_price = decimal(12, 2, nullable=True)


class OrderWithPromotion(OrderWithProduct):
    promotion_name = string(nullable=True)
    promotion_discount = decimal(12, 2, nullable=True)


class OrderFulfillment(OrderWithPromotion):
    shipment_line = integer(nullable=False)
    carrier = string(nullable=True)
    tracking_number = string(nullable=True)
    shipped_at = timestamp(nullable=True)


class OrderPublication(Schema):
    tenant = struct(TenantKey, nullable=False)
    business = struct(BusinessDate, nullable=False)
    id = string(nullable=False)
    line_number = integer(nullable=False)
    customer_id = string(nullable=False)
    customer_name = string(nullable=True)
    customer_tier = string(nullable=True)
    product_name = string(nullable=True)
    product_category = string(nullable=True)
    promotion_name = string(nullable=True)
    total = decimal(12, 2, nullable=False)
    discount = decimal(12, 2, nullable=False)
    net_total = decimal(12, 2, nullable=False)
    quantity = long(nullable=False)
    carrier = string(nullable=True)
    tracking_number = string(nullable=True)
    shipped_at = timestamp(nullable=True)
    is_large = boolean(nullable=False)


class PublicationFlags(Schema):
    has_promotion = boolean(nullable=False)


class OrderPublished(OrderPublication, PublicationFlags):
    pass


class OrderCustomerReconciliation(Schema):
    tenant_id = string(nullable=True)
    order_id = string(nullable=True)
    order_customer_id = string(nullable=True)
    customer_id = string(nullable=True)
    customer_name = string(nullable=True)
    match_status = string(nullable=True)


class CustomerOrderBackfill(Schema):
    tenant_id = string(nullable=True)
    order_id = string(nullable=True)
    order_customer_id = string(nullable=True)
    customer_id = string(nullable=True)
    customer_name = string(nullable=True)
    customer_region = string(nullable=True)


class OrderProductCandidate(Schema):
    tenant_id = string(nullable=True)
    order_id = string(nullable=True)
    customer_id = string(nullable=True)
    customer_name = string(nullable=True)
    product_id = string(nullable=True)
    product_name = string(nullable=True)
