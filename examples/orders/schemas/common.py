from structure import Schema
from structure.platform.pyspark import *


class TenantKey(Schema):
    tenant_id = string(nullable=False)


class AuditStamp(Schema):
    source_system = string(nullable=False)
    ingested_at = timestamp(nullable=False)


class Address(Schema):
    line1 = string(nullable=False)
    line2 = string(nullable=True)
    city = string(nullable=False)
    state = string(nullable=True)
    postal_code = string(nullable=False)
    country = string(nullable=False)


class BusinessDate(Schema):
    order_date = date(nullable=True)
