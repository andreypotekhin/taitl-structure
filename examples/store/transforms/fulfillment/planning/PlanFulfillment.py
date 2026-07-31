from examples.store.schemas.fulfillment.demand import OrderDemand
from examples.store.schemas.fulfillment.planning.intermediate import (
    FulfillmentOption,
    FulfillmentPreferredOption,
    InboundInventoryAvailability,
)
from examples.store.schemas.fulfillment.planning.inventory import InboundInventory, InventoryPosition, Warehouse
from examples.store.schemas.fulfillment.planning.plan import (
    FulfillmentAllocation,
    FulfillmentBackorder,
    FulfillmentPlan,
    ReplenishmentSuggestion,
)
from structure import *
from structure.plugin.pyspark import *


class PlanFulfillment(Transform):
    demand = input(OrderDemand)
    warehouses = input(Warehouse)
    inventory_positions = input(InventoryPosition)
    inbound_inventory = input(InboundInventory)
    inbound_availability = lane(InboundInventoryAvailability)
    options = lane(FulfillmentOption)
    prioritized_options = lane(FulfillmentPreferredOption)
    preferred_options = lane(FulfillmentPreferredOption)
    allocations = output(FulfillmentAllocation)
    backorders = output(FulfillmentBackorder)
    plans = output(FulfillmentPlan)
    replenishment_suggestions = output(ReplenishmentSuggestion)

    @step(input=inbound_inventory, output=inbound_availability)
    def summarize_inbound(self, inbound: InboundInventory) -> InboundInventoryAvailability:
        group_by(
            tenant_id=inbound.tenant.tenant_id,
            warehouse_id=inbound.warehouse_id,
            product_id=inbound.product_id,
        )
        return InboundInventoryAvailability.project(inbound)(
            earliest_expected_at=min(inbound.expected_at),
            expected_quantity=sum(inbound.expected_quantity),
        )

    @step(input=[demand, warehouses, inventory_positions, inbound_availability], output=options)
    def build_options(
        self,
        demand: OrderDemand,
        warehouse: Warehouse,
        inventory: InventoryPosition,
        inbound: InboundInventoryAvailability,
    ) -> FulfillmentOption:
        inner_join(
            warehouse,
            on=(warehouse.tenant.tenant_id == demand.tenant.tenant_id) & warehouse.active,
        )
        inner_join(
            inventory,
            on=(inventory.tenant.tenant_id == demand.tenant.tenant_id)
            & (inventory.warehouse_id == warehouse.id)
            & (inventory.product_id == demand.product_id),
        )
        left_join(
            inbound,
            on=(inbound.tenant.tenant_id == demand.tenant.tenant_id)
            & (inbound.warehouse_id == warehouse.id)
            & (inbound.product_id == demand.product_id),
        )
        available = inventory.on_hand_quantity - inventory.reserved_quantity
        return FulfillmentOption.project(demand)(
            warehouse_id=warehouse.id,
            warehouse_region=warehouse.region,
            warehouse_priority=warehouse.priority,
            available_to_promise=when(available > 0, available).otherwise(0),
            safety_stock_quantity=inventory.safety_stock_quantity,
            earliest_inbound_at=inbound.earliest_expected_at,
            expected_inbound_quantity=coalesce(inbound.expected_quantity, 0),
        )

    @step(input=options, output=prioritized_options)
    def prioritize_options(self, option: FulfillmentOption) -> FulfillmentPreferredOption:
        return FulfillmentPreferredOption.project(option)(
            option_ordinal=row_number(
                partition_by=(option.tenant.tenant_id, option.order_id, option.product_id),
                order_by=(
                    when(option.warehouse_region == option.customer_region, 0).otherwise(1).asc(),
                    option.warehouse_priority.asc(),
                    option.available_to_promise.desc(),
                    option.warehouse_id.asc(),
                ),
            )
        )

    @step(input=prioritized_options, output=preferred_options)
    def select_preferred_option(self, option: FulfillmentPreferredOption) -> FulfillmentPreferredOption:
        where(option.option_ordinal == 1)
        return FulfillmentPreferredOption.project(option)

    @step(input=preferred_options, output=allocations)
    def allocate(self, option: FulfillmentPreferredOption) -> FulfillmentAllocation:
        where(option.available_to_promise > 0)
        allocated = when(option.available_to_promise >= option.requested_quantity, option.requested_quantity)
        return FulfillmentAllocation.project(option)(
            allocated_quantity=allocated.otherwise(option.available_to_promise),
            planned_ship_date=option.business.order_date,
        )

    @step(input=preferred_options, output=backorders)
    def backorder(self, option: FulfillmentPreferredOption) -> FulfillmentBackorder:
        where(option.available_to_promise < option.requested_quantity)
        allocated = when(option.available_to_promise > 0, option.available_to_promise).otherwise(0)
        planned_ship_date = when(
            option.available_to_promise >= option.requested_quantity,
            option.business.order_date,
        )
        return FulfillmentBackorder.project(option)(
            warehouse_id=when(allocated > 0, option.warehouse_id).otherwise(None),
            backordered_quantity=option.requested_quantity - allocated,
            planned_ship_date=planned_ship_date.otherwise(option.earliest_inbound_at),
            reason=when(option.earliest_inbound_at.is_not_null(), "waiting_for_inbound").otherwise(
                "no_available_inventory"
            ),
        )

    @step(input=preferred_options, output=plans)
    def plan(self, option: FulfillmentPreferredOption) -> FulfillmentPlan:
        allocated = when(option.available_to_promise >= option.requested_quantity, option.requested_quantity)
        allocated = allocated.otherwise(
            when(option.available_to_promise > 0, option.available_to_promise).otherwise(0)
        )
        backordered = option.requested_quantity - allocated
        planned_ship_date = when(backordered == 0, option.business.order_date).otherwise(option.earliest_inbound_at)
        status = when(backordered == 0, "allocated")
        return FulfillmentPlan.project(option)(
            allocated_quantity=allocated,
            backordered_quantity=backordered,
            selected_warehouse_id=when(allocated > 0, option.warehouse_id).otherwise(None),
            planned_ship_date=planned_ship_date,
            is_fully_allocated=backordered == 0,
            plan_status=status.otherwise(when(allocated > 0, "partially_allocated").otherwise("backordered")),
        )

    @step(input=preferred_options, output=replenishment_suggestions)
    def suggest_replenishment(self, option: FulfillmentPreferredOption) -> ReplenishmentSuggestion:
        allocated = when(option.available_to_promise >= option.requested_quantity, option.requested_quantity)
        allocated = allocated.otherwise(
            when(option.available_to_promise > 0, option.available_to_promise).otherwise(0)
        )
        after_plan = option.available_to_promise - allocated
        where(
            (after_plan < option.safety_stock_quantity)
            & (
                option.earliest_inbound_at.is_null()
                | (option.earliest_inbound_at > option.business.order_date)
            )
        )
        return ReplenishmentSuggestion.project(option)(
            available_to_promise_after_plan=after_plan,
            reason=when(option.earliest_inbound_at.is_null(), "backorder_without_inbound").otherwise(
                "below_safety_stock_after_allocation"
            ),
        )
