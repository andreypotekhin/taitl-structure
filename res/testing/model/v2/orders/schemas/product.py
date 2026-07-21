from testing.model.v2.orders.schemas.common import AuditStamp, TenantKey

from structure import Schema
from structure.plugin.pyspark import *


class ProductBase(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)


class Product(ProductBase):
    id = string(nullable=False)
    name = string(nullable=True)
    category = string(nullable=True)
    active = boolean(nullable=False)
    list_price = decimal(12, 2, nullable=True)
    weight = float(nullable=True)
    rating = double(nullable=True)


class BlockedProduct(Product):
    reason = string(nullable=True)
