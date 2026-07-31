from examples.store.schemas.common import TenantKey
from examples.store.schemas.fulfillment.demand.demand import OrderDemand
from structure import Schema
from structure.plugin.pyspark import *


class InboundInventoryAvailability(Schema):
    tenant = struct(TenantKey, nullable=False)
    warehouse_id = string(nullable=False)
    product_id = string(nullable=False)
    earliest_expected_at = date(nullable=True)
    expected_quantity = long(nullable=False)


class FulfillmentOption(OrderDemand):
    warehouse_id = string(nullable=False)
    warehouse_region = string(nullable=True)
    warehouse_priority = integer(nullable=False)
    available_to_promise = long(nullable=False)
    safety_stock_quantity = long(nullable=False)
    earliest_inbound_at = date(nullable=True)
    expected_inbound_quantity = long(nullable=False)


class FulfillmentPreferredOption(FulfillmentOption):
    option_ordinal = long(nullable=False)
