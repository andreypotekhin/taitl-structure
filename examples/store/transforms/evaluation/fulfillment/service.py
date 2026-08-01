from examples.store.schemas.fulfillment.evaluation import (
    DailyFulfillmentServiceSummary,
    FulfillmentServiceEvaluation,
    FulfillmentServiceTotals,
)
from examples.store.schemas.fulfillment.planning.plan import FulfillmentPlan
from examples.store.schemas.fulfillment.shortages import ServiceRiskTarget
from examples.store.schemas.order import OrderFulfillment
from structure import *
from structure.plugin.pyspark import *


class EvaluateFulfillment(Transform):
    """Compare each planned line with observed shipment facts, preserving unknown dates."""

    plans = input(FulfillmentPlan)
    fulfilled = input(OrderFulfillment)
    service_targets = input(ServiceRiskTarget)
    totals = lane(FulfillmentServiceTotals)
    evaluations = lane(FulfillmentServiceEvaluation)
    summary_totals = lane(DailyFulfillmentServiceSummary)
    service_evaluations = output(FulfillmentServiceEvaluation)
    daily_summary = output(DailyFulfillmentServiceSummary)
    result = output(service_evaluations=evaluations)

    @step(input=[plans, fulfilled, service_targets], output=totals)
    def evaluate(
        self, plan: FulfillmentPlan, fulfilled: OrderFulfillment, target: ServiceRiskTarget
    ) -> FulfillmentServiceTotals:
        left_join(
            fulfilled,
            on=(fulfilled.tenant.tenant_id == plan.tenant.tenant_id)
            & (fulfilled.id == plan.order_id)
            & (fulfilled.line_number == plan.line_number),
        )
        left_join(target, on=(target.tenant.tenant_id == plan.tenant.tenant_id) & target.active)
        group_by(
            tenant_id=plan.tenant.tenant_id,
            business=plan.business,
            order_id=plan.order_id,
            line_number=plan.line_number,
            product_id=plan.product_id,
            selected_warehouse_id=plan.selected_warehouse_id,
            target_id=target.target_id,
            target_on_time=target.on_time_target,
            requested_quantity=plan.requested_quantity,
            planned_quantity=plan.allocated_quantity,
            planned_ship_date=plan.planned_ship_date,
        )
        shipped_quantity = sum(coalesce(fulfilled.quantity, 0))
        shipped = bool_or(fulfilled.line_number.is_not_null())
        actual_ship_date = max(to_date(fulfilled.shipped_at))
        return FulfillmentServiceTotals.project(plan)(
            target_id=target.target_id,
            target_on_time=target.on_time_target,
            planned_quantity=plan.allocated_quantity,
            shipped_quantity=shipped_quantity,
            actual_ship_date=actual_ship_date,
            shipped=shipped,
        )

    @step(input=totals, output=evaluations)
    def classify(self, total: FulfillmentServiceTotals) -> FulfillmentServiceEvaluation:
        on_time_status = when(total.actual_ship_date.is_null(), "unknown").otherwise(
            when(total.planned_ship_date.is_null(), "unknown")
            .otherwise(when(total.actual_ship_date <= total.planned_ship_date, "on_time").otherwise("late"))
        )
        in_full_status = when(~total.shipped, "unknown").otherwise(
            when(total.shipped_quantity >= total.planned_quantity, "in_full").otherwise("partial")
        )
        service_status = when(~total.shipped, "not_shipped").otherwise(
            when((on_time_status == "on_time") & (in_full_status == "in_full"), "on_time_in_full")
            .otherwise(
                when((on_time_status == "late") & (in_full_status == "in_full"), "late_in_full")
                .otherwise(when(on_time_status == "on_time", "on_time_partial").otherwise("late_partial"))
            )
        )
        return FulfillmentServiceEvaluation.project(total)(
            on_time_status=on_time_status,
            in_full_status=in_full_status,
            lateness_days=when(
                total.actual_ship_date.is_not_null() & total.planned_ship_date.is_not_null(),
                datediff(total.actual_ship_date, total.planned_ship_date),
            ).otherwise(None),
            service_status=service_status,
        )

    @step(input=evaluations, output=summary_totals)
    def summarize(self, evaluation: FulfillmentServiceEvaluation) -> DailyFulfillmentServiceSummary:
        group_by(
            tenant_id=evaluation.tenant.tenant_id,
            business_date=evaluation.business.order_date,
            warehouse_id=evaluation.selected_warehouse_id,
            target_id=evaluation.target_id,
            target_on_time=evaluation.target_on_time,
        )
        return DailyFulfillmentServiceSummary.project(evaluation)(
            business_date=evaluation.business.order_date,
            warehouse_id=evaluation.selected_warehouse_id,
            evaluated_line_count=count(),
            on_time_in_full_count=sum(when(evaluation.service_status == "on_time_in_full", 1).otherwise(0)),
            on_time_line_count=sum(when(evaluation.on_time_status == "on_time", 1).otherwise(0)),
            in_full_line_count=sum(when(evaluation.in_full_status == "in_full", 1).otherwise(0)),
            service_level=sum(0.0),
            target_attained=bool_or(evaluation.service_status == "on_time_in_full"),
        )

    @step(input=summary_totals, output=daily_summary)
    def publish_summary(self, summary: DailyFulfillmentServiceSummary) -> DailyFulfillmentServiceSummary:
        service_level = when(summary.evaluated_line_count > 0, summary.on_time_in_full_count / summary.evaluated_line_count).otherwise(None)
        return DailyFulfillmentServiceSummary.project(summary)(
            service_level=service_level,
            target_attained=when(summary.target_on_time.is_not_null(), service_level >= summary.target_on_time).otherwise(None),
        )
