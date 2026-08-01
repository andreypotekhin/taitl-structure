from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class DemandWindow(Schema):
    """Observed demand grouped into a bounded date interval."""

    tenant = struct(TenantKey, nullable=False)
    product_id = string(nullable=False)
    window_start = date(nullable=False)
    window_end = date(nullable=False)
    requested_quantity = long(nullable=False)
    demand_line_count = long(nullable=False)
