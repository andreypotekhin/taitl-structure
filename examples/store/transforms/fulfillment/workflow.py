from examples.store.schemas.customer import Customer
from examples.store.schemas.fulfillment import (
    DailyFulfillmentServiceSummary,
    DailyFulfillmentSummary,
    DemandWindow,
    FulfillmentAllocation,
    FulfillmentBackorder,
    FulfillmentException,
    FulfillmentPlan,
    FulfillmentReconciliation,
    FulfillmentServiceEvaluation,
    FulfillmentShortage,
    FulfillmentSubstitutionOption,
    InboundInventory,
    InventoryPosition,
    InventoryProjection,
    LeadTime,
    Order,
    ReplenishmentSuggestion,
    ServiceRiskTarget,
    SubstitutionRule,
    Warehouse,
    WarehouseLoadSummary,
)
from examples.store.schemas.order import OrderFulfillment, OrderRaw
from examples.store.schemas.product import BlockedProduct, Product
from examples.store.schemas.promotion import Promotion
from examples.store.transforms.fulfillment.demand import PrepareOrderDemand
from examples.store.transforms.fulfillment.evaluation import EvaluateFulfillmentService
from examples.store.transforms.fulfillment.plan import PlanFulfillment
from examples.store.transforms.fulfillment.projections import BuildDemandWindows, ProjectInventory
from examples.store.transforms.fulfillment.reconcile import ReconcileFulfillmentPlan
from examples.store.transforms.fulfillment.shortages import DetectFulfillmentShortages, PrioritizeFulfillmentExceptions
from examples.store.transforms.fulfillment.substitutions import FindFulfillmentSubstitutions
from examples.store.transforms.fulfillment.summarize import FulfillmentAnalytics
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
    lead_times = input(LeadTime)
    substitution_rules = input(SubstitutionRule)
    service_targets = input(ServiceRiskTarget)
    fulfilled = input(OrderFulfillment)

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
    windows = stage(BuildDemandWindows(demand=prepared.demand))
    projections = stage(
        ProjectInventory(
            windows=windows.windows,
            inventory_positions=inventory_positions,
            inbound_inventory=inbound_inventory,
            lead_times=lead_times,
        )
    )
    shortage_stage = stage(DetectFulfillmentShortages(projections=projections.projections))
    substitution_stage = stage(
        FindFulfillmentSubstitutions(
            demand=prepared.demand,
            rules=substitution_rules,
            inventory_positions=inventory_positions,
        )
    )
    exception_stage = stage(
        PrioritizeFulfillmentExceptions(
            shortages=shortage_stage.shortages,
            plans=planned.plans,
            demand=prepared.demand,
            substitutions=substitution_stage.options,
            service_targets=service_targets,
        )
    )
    reconciled = stage(
        ReconcileFulfillmentPlan(
            plans=planned.plans,
            fulfilled=fulfilled,
        )
    )
    summarized = stage(
        FulfillmentAnalytics(
            plans=planned.plans,
            allocations=planned.allocations,
            backorders=planned.backorders,
        )
    )
    evaluated = stage(
        EvaluateFulfillmentService(
            plans=planned.plans,
            fulfilled=fulfilled,
            service_targets=service_targets,
        )
    )

    demand = output(Order, prepared.demand)
    allocations = output(FulfillmentAllocation, planned.allocations)
    backorders = output(FulfillmentBackorder, planned.backorders)
    plans = output(FulfillmentPlan, planned.plans)
    reconciliation = output(FulfillmentReconciliation, reconciled.reconciliation)
    replenishment_suggestions = output(ReplenishmentSuggestion, planned.replenishment_suggestions)
    daily_summary = output(DailyFulfillmentSummary, summarized.daily_summary)
    warehouse_load_summary = output(WarehouseLoadSummary, summarized.warehouse_load_summary)
    demand_windows = output(DemandWindow, windows.windows)
    inventory_projections = output(InventoryProjection, projections.projections)
    shortages = output(FulfillmentShortage, shortage_stage.shortages)
    substitution_options = output(FulfillmentSubstitutionOption, substitution_stage.options)
    exceptions = output(FulfillmentException, exception_stage.exceptions)
    service_evaluations = output(FulfillmentServiceEvaluation, evaluated.service_evaluations)
    daily_service_summary = output(DailyFulfillmentServiceSummary, evaluated.daily_summary)
