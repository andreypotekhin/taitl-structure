from examples.store.schemas.fulfillment.projections import InventoryProjection
from examples.store.schemas.fulfillment.shortages import FulfillmentShortage, FulfillmentShortageRanked
from structure import *
from structure.plugin.pyspark import *


class DetectShortages(Transform):
    """Publish the first below-safety projection for each warehouse and product."""

    projections = input(InventoryProjection)
    ranked = lane(FulfillmentShortageRanked)
    shortages = output(FulfillmentShortage)

    @step(input=projections, output=ranked)
    def identify(self, projection: InventoryProjection) -> FulfillmentShortageRanked:
        where(projection.projected_available_quantity < projection.safety_stock_quantity)
        return FulfillmentShortageRanked.project(projection)(
            first_shortage_at=projection.window_start,
            shortage_quantity=projection.safety_stock_quantity - projection.projected_available_quantity,
            reason="below_safety_stock",
            shortage_ordinal=row_number(
                partition_by=(projection.tenant.tenant_id, projection.warehouse_id, projection.product_id),
                order_by=projection.window_start.asc(),
            ),
        )

    @step(input=ranked, output=shortages)
    def select_first(self, shortage: FulfillmentShortageRanked) -> FulfillmentShortage:
        where(shortage.shortage_ordinal == 1)
        return FulfillmentShortage.project(shortage)
