from testing.model.v3.orders.schemas.common import AuditStamp, TenantKey

from structure import Schema
from structure.plugin.pyspark import *


class Customer(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    id = string(nullable=False)
    name = string(nullable=True)
    tier = string(nullable=True)
    region = string(nullable=True)
    email = string(nullable=True)
