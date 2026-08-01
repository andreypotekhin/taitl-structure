from examples.store.schemas.common import AuditStamp, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class Warehouse(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    id = string(nullable=False)
    name = string(nullable=True)
    region = string(nullable=True)
    priority = integer(nullable=False)
    active = boolean(nullable=False)
