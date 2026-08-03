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
from examples.store.transforms.analytics.fulfillment import FulfillmentAnalytics
from examples.store.transforms.evaluation.fulfillment import EvaluateFulfillment
from examples.store.transforms.fulfillment.demand import BuildDemandWindows, PrepareOrderDemand
from examples.store.transforms.fulfillment.inventory import ProjectInventory
from examples.store.transforms.fulfillment.planning import PlanFulfillment
from examples.store.transforms.fulfillment.reconciliation import ReconcileFulfillmentPlan
from examples.store.transforms.fulfillment.shortages import DetectShortages, PrioritizeExceptions
from examples.store.transforms.fulfillment.substitutions import FindSubstitutions
from structure import Transform, input, output, transform


@transform
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
    demand = output(Order)
    allocations = output(FulfillmentAllocation)
    backorders = output(FulfillmentBackorder)
    plans = output(FulfillmentPlan)
    reconciliation = output(FulfillmentReconciliation)
    replenishment_suggestions = output(ReplenishmentSuggestion)
    daily_summary = output(DailyFulfillmentSummary)
    warehouse_load_summary = output(WarehouseLoadSummary)
    demand_windows = output(DemandWindow)
    inventory_projections = output(InventoryProjection)
    shortages = output(FulfillmentShortage)
    substitution_options = output(FulfillmentSubstitutionOption)
    exceptions = output(FulfillmentException)
    service_evaluations = output(FulfillmentServiceEvaluation)
    daily_service_summary = output(DailyFulfillmentServiceSummary)

    prepared = PrepareOrderDemand(
        orders=orders,
        customers=customers,
        products=products,
        blocked_products=blocked_products,
        promotions=promotions,
    )

    planned = PlanFulfillment(
        demand=prepared.demand,
        warehouses=warehouses,
        inventory_positions=inventory_positions,
        inbound_inventory=inbound_inventory,
    )

    windows = BuildDemandWindows(demand=prepared.demand)

    inventory_projection = ProjectInventory(
        windows=windows.windows,
        inventory_positions=inventory_positions,
        inbound_inventory=inbound_inventory,
        lead_times=lead_times,
    )

    shortage_stage = DetectShortages(projections=inventory_projection.projections)

    substitution_stage = FindSubstitutions(
        demand=prepared.demand,
        rules=substitution_rules,
        inventory_positions=inventory_positions,
    )

    exception_stage = PrioritizeExceptions(
        shortages=shortage_stage.shortages,
        plans=planned.plans,
        demand=prepared.demand,
        substitutions=substitution_stage.options,
        service_targets=service_targets,
    )

    reconciled = ReconcileFulfillmentPlan(
        plans=planned.plans,
        fulfilled=fulfilled,
    )

    summarized = FulfillmentAnalytics(
        plans=planned.plans,
        allocations=planned.allocations,
        backorders=planned.backorders,
    )

    evaluated = EvaluateFulfillment(
        plans=planned.plans,
        fulfilled=fulfilled,
        service_targets=service_targets,
    )

    result = output(
        demand=prepared.demand,
        allocations=planned.allocations,
        backorders=planned.backorders,
        plans=planned.plans,
        reconciliation=reconciled.reconciliation,
        replenishment_suggestions=planned.replenishment_suggestions,
        daily_summary=summarized.daily_summary,
        warehouse_load_summary=summarized.warehouse_load_summary,
        demand_windows=windows.windows,
        inventory_projections=inventory_projection.projections,
        shortages=shortage_stage.shortages,
        substitution_options=substitution_stage.options,
        exceptions=exception_stage.exceptions,
        service_evaluations=evaluated.service_evaluations,
        daily_service_summary=evaluated.daily_summary,
    )
