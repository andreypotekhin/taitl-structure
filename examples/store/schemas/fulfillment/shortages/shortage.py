from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class FulfillmentShortage(Schema):
    tenant = struct(TenantKey, nullable=False)
    warehouse_id = string(nullable=False)
    product_id = string(nullable=False)
    first_shortage_at = date(nullable=False)
    shortage_quantity = long(nullable=False)
    projected_available_quantity = long(nullable=False)
    safety_stock_quantity = long(nullable=False)
    reason = string(nullable=False)


class FulfillmentShortageRanked(FulfillmentShortage):
    shortage_ordinal = long(nullable=False)
