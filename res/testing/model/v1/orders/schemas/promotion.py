from structure import Schema
from structure.plugin.pyspark import *

from testing.model.v1.orders.schemas.common import AuditStamp, TenantKey


class Promotion(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    code = string(nullable=False)
    name = string(nullable=True)
    discount = decimal(12, 2, nullable=True)
