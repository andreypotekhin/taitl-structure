from examples.store.schemas.customer import Customer
from examples.store.schemas.fulfillment import (
    DailyFulfillmentSummary,
    FulfillmentAllocation,
    FulfillmentBackorder,
    FulfillmentPlan,
    InboundInventory,
    InventoryPosition,
    OrderDemand,
    ReplenishmentSuggestion,
    Warehouse,
    WarehouseLoadSummary,
)
from examples.store.schemas.order import OrderRaw
from examples.store.schemas.product import BlockedProduct, Product
from examples.store.schemas.promotion import Promotion
from examples.store.transforms.fulfillment.analytics import FulfillmentAnalytics
from examples.store.transforms.fulfillment.demand import PrepareOrderDemand
from examples.store.transforms.fulfillment.planning import PlanFulfillment
from structure import Transform, input, output, stage


class Fulfillment(Transform):
    orders = input(OrderRaw, streaming=True)
    customers = input(Customer)
    products = input(Product)
    blocked_products = input(BlockedProduct)
    promotions = input(Promotion)
    warehouses = input(Warehouse)
    inventory_positions = input(InventoryPosition)
    inbound_inventory = input(InboundInventory)

    prepared = stage(
        PrepareOrderDemand(
            orders=orders,
            customers=customers,
            products=products,
            blocked_products=blocked_products,
            promotions=promotions,
        )
    )
    planned = stage(
        PlanFulfillment(
            demand=prepared.demand,
            warehouses=warehouses,
            inventory_positions=inventory_positions,
            inbound_inventory=inbound_inventory,
        )
    )
    summarized = stage(
        FulfillmentAnalytics(
            plans=planned.plans,
            allocations=planned.allocations,
            backorders=planned.backorders,
        )
    )

    demand = output(OrderDemand, prepared.demand)
    allocations = output(FulfillmentAllocation, planned.allocations)
    backorders = output(FulfillmentBackorder, planned.backorders)
    plans = output(FulfillmentPlan, planned.plans)
    replenishment_suggestions = output(ReplenishmentSuggestion, planned.replenishment_suggestions)
    daily_summary = output(DailyFulfillmentSummary, summarized.daily_summary)
    warehouse_load_summary = output(WarehouseLoadSummary, summarized.warehouse_load_summary)
