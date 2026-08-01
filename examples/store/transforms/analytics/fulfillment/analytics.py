from examples.store.schemas.fulfillment.analytics.summary import DailyFulfillmentSummary, WarehouseLoadSummary
from examples.store.schemas.fulfillment.planning.plan import (
    FulfillmentAllocation,
    FulfillmentBackorder,
    FulfillmentPlan,
)
from structure import *
from structure.plugin.pyspark import *


class FulfillmentAnalytics(Transform):
    plans = input(FulfillmentPlan)
    allocations = input(FulfillmentAllocation)
    backorders = input(FulfillmentBackorder)
    daily_totals = lane(DailyFulfillmentSummary)
    daily_summary = output(DailyFulfillmentSummary)
    warehouse_load_summary = output(WarehouseLoadSummary)

    @step(input=plans, output=daily_totals)
    def summarize_daily_totals(self, plan: FulfillmentPlan) -> DailyFulfillmentSummary:
        group_by(
            tenant_id=plan.tenant.tenant_id,
            business_date=plan.business.order_date,
        )
        return DailyFulfillmentSummary.project(plan)(
            business_date=plan.business.order_date,
            demand_line_count=count(),
            allocated_line_count=sum(when(plan.plan_status == "allocated", 1).otherwise(0)),
            partially_allocated_line_count=sum(when(plan.plan_status == "partially_allocated", 1).otherwise(0)),
            backordered_line_count=sum(when(plan.plan_status == "backordered", 1).otherwise(0)),
            requested_units=sum(plan.requested_quantity),
            allocated_units=sum(plan.allocated_quantity),
            backordered_units=sum(plan.backordered_quantity),
            fill_rate=sum(0.0),
            backorder_rate=sum(0.0),
        )

    @step(input=daily_totals, output=daily_summary)
    def publish_daily_summary(self, daily: DailyFulfillmentSummary) -> DailyFulfillmentSummary:
        return DailyFulfillmentSummary.project(daily)(
            fill_rate=when(daily.requested_units > 0, daily.allocated_units / daily.requested_units).otherwise(None),
            backorder_rate=when(daily.requested_units > 0, daily.backordered_units / daily.requested_units).otherwise(
                None
            ),
        )

    @step(input=allocations, output=warehouse_load_summary)
    def summarize_warehouse_load(self, allocation: FulfillmentAllocation) -> WarehouseLoadSummary:
        group_by(
            tenant_id=allocation.tenant.tenant_id,
            warehouse_id=allocation.warehouse_id,
            business_date=allocation.business.order_date,
        )
        return WarehouseLoadSummary.project(allocation)(
            business_date=allocation.business.order_date,
            planned_line_count=count(),
            allocated_units=sum(allocation.allocated_quantity),
            distinct_products=count_distinct(allocation.product_id),
            customer_count=count_distinct(allocation.customer_id),
        )
