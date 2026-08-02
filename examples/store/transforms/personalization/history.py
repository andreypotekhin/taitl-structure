from examples.store.schemas.merchandising import SessionEvent
from examples.store.schemas.order import OrderFulfillment
from examples.store.schemas.personalization import PersonalizationHistory
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import group_by, sum, union_all, when, where


class BuildPersonalizationHistory(Transform):
    """Normalize browsing and purchase facts into personal-history interactions."""

    session_events = input(SessionEvent, streaming=True)
    fulfilled_orders = input(OrderFulfillment, streaming=True)
    session_history = lane(PersonalizationHistory)
    purchase_history = lane(PersonalizationHistory)
    history = output(PersonalizationHistory)

    @step(input=session_events, output=session_history)
    def browse(self, event: SessionEvent) -> PersonalizationHistory:
        where(event.product_id.is_not_null())
        group_by(
            tenant_id=event.tenant.tenant_id,
            customer_id=event.customer_id,
            session_id=event.session_id,
            product_id=event.product_id,
            category=event.category,
        )
        return PersonalizationHistory.project(event)(
            customer_id=event.customer_id,
            session_id=event.session_id,
            product_id=event.product_id,
            category=event.category,
            history_score=sum(
                when(event.event_type == "product_view", 1.0)
                .otherwise(when(event.event_type == "add_to_cart", 3.0).otherwise(2.0))
            ),
        )

    @step(input=fulfilled_orders, output=purchase_history)
    def purchase(self, order: OrderFulfillment) -> PersonalizationHistory:
        group_by(
            tenant_id=order.tenant.tenant_id,
            customer_id=order.customer_id,
            session_id=None,
            product_id=order.product_id,
            category=order.product_category,
        )
        return PersonalizationHistory.project(order)(
            customer_id=order.customer_id,
            session_id=None,
            product_id=order.product_id,
            category=order.product_category,
            history_score=sum(5.0),
        )

    @step(input=[session_history, purchase_history], output=history)
    def merge(
        self, session: PersonalizationHistory, purchase: PersonalizationHistory
    ) -> PersonalizationHistory:
        merged = union_all(purchase)
        group_by(
            tenant_id=session.tenant.tenant_id,
            customer_id=merged.customer_id,
            session_id=merged.session_id,
            product_id=merged.product_id,
            category=merged.category,
        )
        return PersonalizationHistory.project(merged)(history_score=sum(merged.history_score))
