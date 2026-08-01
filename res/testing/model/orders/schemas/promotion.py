from testing.model.orders.schemas.common import AuditStamp, TenantKey

from structure import Schema
from structure.plugin.pyspark import *


class Promotion(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    code = string(nullable=False)
    name = string(nullable=True)
    discount = decimal(12, 2, nullable=True)
    valid_from = date(nullable=False)
    valid_to = date(nullable=True)
