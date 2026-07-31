from examples.store.schemas.common import BusinessDate, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class Order(Schema):
    tenant = struct(TenantKey, nullable=False)
    business = struct(BusinessDate, nullable=False)
    order_id = string(nullable=False)
    line_number = integer(nullable=False)
    customer_id = string(nullable=False)
    customer_name = string(nullable=True)
    customer_tier = string(nullable=True)
    customer_region = string(nullable=True)
    product_id = string(nullable=False)
    product_name = string(nullable=True)
    product_category = string(nullable=True)
    promotion_code = string(nullable=True)
    promotion_name = string(nullable=True)
    promotion_discount = decimal(12, 2, nullable=True)
    total = decimal(12, 2, nullable=False)
    discount = decimal(12, 2, nullable=False)
    net_total = decimal(12, 2, nullable=False)
    requested_quantity = long(nullable=False)
    is_large = boolean(nullable=False)


# Compatibility name for callers using the original demand-oriented name.
OrderDemand = Order
