from examples.store.schemas.fulfillment.inventory import InboundInventory, InventoryPosition, LeadTime
from examples.store.schemas.fulfillment.projections import DemandWindow, InventoryProjection
from structure import *
from structure.plugin.pyspark import *


class ProjectInventory(Transform):
    """Project inventory across observed demand windows without forecasting demand."""

    windows = input(DemandWindow)
    inventory_positions = input(InventoryPosition)
    inbound_inventory = input(InboundInventory)
    lead_times = input(LeadTime)
    inbound_facts = lane(InboundInventory)
    projections = output(InventoryProjection)

    @step(input=inbound_inventory, output=inbound_facts)
    def summarize_inbound(self, inbound: InboundInventory) -> InboundInventory:
        group_by(
            tenant_id=inbound.tenant.tenant_id,
            audit=inbound.audit,
            warehouse_id=inbound.warehouse_id,
            product_id=inbound.product_id,
            expected_at=inbound.expected_at,
            source_type=inbound.source_type,
        )
        return InboundInventory.project(inbound)(expected_quantity=sum(inbound.expected_quantity))

    @step(input=[windows, inventory_positions, inbound_facts, lead_times], output=projections)
    def project_inventory(
        self,
        demand: DemandWindow,
        inventory: InventoryPosition,
        inbound: InboundInventory,
        lead_time: LeadTime,
    ) -> InventoryProjection:
        inner_join(
            inventory,
            on=(inventory.tenant.tenant_id == demand.tenant.tenant_id)
            & (inventory.product_id == demand.product_id)
            & (demand.window_start >= inventory.as_of),
        )
        left_join(
            inbound,
            on=(inbound.tenant.tenant_id == inventory.tenant.tenant_id)
            & (inbound.warehouse_id == inventory.warehouse_id)
            & (inbound.product_id == inventory.product_id)
            & (inbound.expected_at == demand.window_start),
        )
        left_join(
            lead_time,
            on=(lead_time.tenant.tenant_id == inventory.tenant.tenant_id)
            & (lead_time.warehouse_id == inventory.warehouse_id)
            & (lead_time.product_id == inventory.product_id)
            & lead_time.active,
        )
        opening = inventory.on_hand_quantity - inventory.reserved_quantity
        inbound_quantity = coalesce(inbound.expected_quantity, 0)
        delta = inbound_quantity - demand.requested_quantity
        running_delta = window_sum(
            delta,
            over=window(
                partition_by=(inventory.tenant.tenant_id, inventory.warehouse_id, inventory.product_id),
                order_by=(demand.window_start.asc(), demand.window_end.asc()),
                frame=rows_between(unbounded_preceding(), current_row()),
            ),
        )
        usable_at = date_add(demand.window_start, days=coalesce(lead_time.days, 0))
        return InventoryProjection(
            tenant=inventory.tenant,
            warehouse_id=inventory.warehouse_id,
            product_id=inventory.product_id,
            window_start=demand.window_start,
            window_end=demand.window_end,
            usable_at=usable_at,
            opening_quantity=opening,
            inbound_quantity=inbound_quantity,
            demand_quantity=demand.requested_quantity,
            reserved_quantity=inventory.reserved_quantity,
            projected_available_quantity=opening + running_delta,
            safety_stock_quantity=inventory.safety_stock_quantity,
        )
