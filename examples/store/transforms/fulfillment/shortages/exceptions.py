from examples.store.schemas.fulfillment.demand import Order
from examples.store.schemas.fulfillment.planning.plan import FulfillmentPlan
from examples.store.schemas.fulfillment.shortages import FulfillmentException, FulfillmentShortage, ServiceRiskTarget
from examples.store.schemas.fulfillment.substitutions import FulfillmentSubstitutionOption
from structure import *
from structure.plugin.pyspark import *


class PrioritizeExceptions(Transform):
    """Combine shortage, service-risk, and substitution signals into a stable work queue."""

    shortages = input(FulfillmentShortage)
    plans = input(FulfillmentPlan)
    demand = input(Order)
    substitutions = input(FulfillmentSubstitutionOption)
    service_targets = input(ServiceRiskTarget)
    shortage_exceptions = lane(FulfillmentException)
    plan_exceptions = lane(FulfillmentException)
    substitution_exceptions = lane(FulfillmentException)
    merged = lane(FulfillmentException)
    exceptions = output(FulfillmentException)

    @step(input=shortages, output=shortage_exceptions)
    def shortage_exception(self, shortage: FulfillmentShortage) -> FulfillmentException:
        return FulfillmentException.project(shortage)(
            business=None,
            order_id=None,
            line_number=None,
            target_id=None,
            reason="shortage",
            severity=3,
            priority_score=shortage.shortage_quantity * 1000,
            priority_rank=0,
            due_date=shortage.first_shortage_at,
            days_until_due=None,
            customer_tier=None,
            recommended_action="review replenishment or an approved substitution",
        )

    @step(input=[plans, demand, service_targets], output=plan_exceptions)
    def plan_exception(
        self, plan: FulfillmentPlan, order: Order, target: ServiceRiskTarget
    ) -> FulfillmentException:
        left_join(
            order,
            on=(order.tenant.tenant_id == plan.tenant.tenant_id)
            & (order.order_id == plan.order_id)
            & (order.line_number == plan.line_number),
        )
        left_join(
            target,
            on=(target.tenant.tenant_id == plan.tenant.tenant_id) & target.active,
        )
        where(plan.plan_status != "allocated")
        days_until_due = datediff(plan.business.order_date, plan.planned_ship_date)
        customer_weight = (
            when(order.customer_tier == "platinum", 4)
            .otherwise(
                when(order.customer_tier == "gold", 3)
                .otherwise(when(order.customer_tier == "silver", 2).otherwise(1))
            )
        )
        lateness_weight = when(days_until_due.is_not_null(), -days_until_due).otherwise(0)
        target_weight = when(coalesce(target.on_time_target, 0.0) > 0.0, 1).otherwise(0)
        reason = when(target_weight > 0, "service_target_at_risk").otherwise(
            when(plan.planned_ship_date.is_not_null() & (plan.planned_ship_date > plan.business.order_date), "late_inbound").otherwise("shortage")
        )
        return FulfillmentException.project(plan)(
            warehouse_id=plan.selected_warehouse_id,
            target_id=target.target_id,
            reason=reason,
            severity=when(plan.backordered_quantity > 0, 3).otherwise(2),
            priority_score=coalesce(
                (plan.backordered_quantity * 1000)
                + (customer_weight * 100)
                + (lateness_weight * 10)
                + target_weight,
                0,
            ),
            priority_rank=0,
            due_date=plan.business.order_date,
            days_until_due=days_until_due,
            shortage_quantity=plan.backordered_quantity,
            customer_tier=order.customer_tier,
            recommended_action="review the plan against the service target",
        )

    @step(input=[substitutions, demand], output=substitution_exceptions)
    def substitution_exception(
        self, option: FulfillmentSubstitutionOption, order: Order
    ) -> FulfillmentException:
        left_join(
            order,
            on=(order.tenant.tenant_id == option.tenant.tenant_id)
            & (order.order_id == option.order_id)
            & (order.line_number == option.line_number),
        )
        return FulfillmentException.project(option)(
            business=order.business,
            product_id=option.original_product_id,
            warehouse_id=None,
            target_id=None,
            reason="substitution_available",
            severity=1,
            priority_score=(100 - option.policy_rank) * 100,
            priority_rank=0,
            due_date=order.business.order_date,
            days_until_due=0,
            shortage_quantity=0,
            customer_tier=order.customer_tier,
            recommended_action="offer the ranked policy-approved substitute",
        )

    @step(
        input=[shortage_exceptions, plan_exceptions, substitution_exceptions],
        output=merged,
    )
    def merge_exceptions(
        self,
        shortage: FulfillmentException,
        plan: FulfillmentException,
        substitution: FulfillmentException,
    ) -> FulfillmentException:
        candidate = union_all(plan)
        candidate = union_all(substitution)
        return FulfillmentException.project(candidate)

    @step(input=merged, output=exceptions)
    def rank_exceptions(self, exception: FulfillmentException) -> FulfillmentException:
        return FulfillmentException.project(exception)(
            priority_rank=row_number(
                partition_by=exception.tenant.tenant_id,
                order_by=(
                    exception.priority_score.desc(),
                    exception.due_date.asc_nulls_last(),
                    exception.product_id.asc_nulls_last(),
                    exception.order_id.asc_nulls_last(),
                ),
            )
        )
