from examples.store.schemas.common import AuditStamp, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class CatalogProduct(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    product_id = string(nullable=False)
    product_name = string(nullable=True)
    category = string(nullable=True)
    active = boolean(nullable=False)
    list_price = decimal(12, 2, nullable=True)
    rating = double(nullable=True)
    has_promotion = boolean(nullable=False)
    promotion_code = string(nullable=True)
    promotion_name = string(nullable=True)
    promotion_discount = decimal(12, 2, nullable=True)
    base_score = double(nullable=False)
    promotion_score = double(nullable=False)
    eligible = boolean(nullable=False)


class CatalogAvailability(Schema):
    tenant = struct(TenantKey, nullable=False)
    product_id = string(nullable=False)
    available_to_promise = long(nullable=False)
    inventory_boost = double(nullable=False)
    availability_status = string(nullable=True)
