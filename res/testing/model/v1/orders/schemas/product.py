from structure import Schema
from structure.plugin.pyspark import *

from testing.model.v1.orders.schemas.common import AuditStamp, TenantKey


class Product(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    id = string(nullable=False)
    name = string(nullable=True)
    category = string(nullable=True)
    active = boolean(nullable=False)
    list_price = decimal(12, 2, nullable=True)
    weight = float(nullable=True)
    rating = double(nullable=True)
