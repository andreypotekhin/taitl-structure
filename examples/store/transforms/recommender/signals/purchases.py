from datetime import datetime as utc_datetime
from datetime import timezone

from examples.store.schemas.merchandising import RecommendationImpression, RecommendationPurchase
from examples.store.schemas.order import OrderFulfillment
from structure import Transform, input, output, transform
from structure.plugin.pyspark import coalesce, event_time_between, left_join, to_timestamp, when


@transform(streaming=True)
class BuildPurchaseSignals(Transform):
    """Attribute fulfilled commercial order facts to recent recommendation exposure."""

    fulfilled_orders = input(OrderFulfillment, streaming=True)
    impressions = input(RecommendationImpression, streaming=True)
    purchases = output(RecommendationPurchase)

    def attribute(self, order: OrderFulfillment, impression: RecommendationImpression) -> RecommendationPurchase:
        occurred_at = coalesce(
            order.shipped_at,
            to_timestamp(order.business.order_date),
            utc_datetime(1970, 1, 1, tzinfo=timezone.utc),
        )
        left_join(
            impression,
            on=(impression.tenant.tenant_id == order.tenant.tenant_id)
            & (impression.product_id == order.product_id)
            & impression.customer_id.null_safe_eq(order.customer_id)
            & event_time_between(impression.shown_at, occurred_at, upper="30 days"),
        )
        attributed = impression.id.is_not_null()
        return RecommendationPurchase.project(order)(
            order_id=order.id,
            request_id=impression.request_id,
            session_id=impression.session_id,
            product_id=order.product_id,
            recommendation_impression_id=impression.id,
            occurred_at=occurred_at,
            attribution_status=when(attributed, "attributed").otherwise("unattributed"),
            quantity=order.quantity,
        )


from datetime import datetime
