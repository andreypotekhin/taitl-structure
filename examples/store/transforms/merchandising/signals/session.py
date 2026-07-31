from examples.store.schemas.merchandising import SessionEvent, SessionFeature
from structure import Transform, input, output, transform
from structure.plugin.pyspark import *


@transform(streaming=True)
class BuildSessionSignals(Transform):
    """Build bounded, deduplicated session interests from event facts."""

    events = input(SessionEvent, streaming=True)
    features = output(SessionFeature)

    def build(self, event: SessionEvent) -> SessionFeature:
        watermark(event.occurred_at, delay="7 days")
        drop_duplicates_within_watermark(event.id)
        day = window(event.occurred_at, "1 day")
        group_by(
            window=day,
            tenant_id=event.tenant.tenant_id,
            session_id=event.session_id,
            customer_id=event.customer_id,
            product_id=event.product_id,
            category=event.category,
        )
        return SessionFeature(
            window=day,
            tenant=event.tenant,
            session_id=event.session_id,
            customer_id=event.customer_id,
            product_id=event.product_id,
            category=event.category,
            event_count=count(),
            product_view_count=sum(when(event.event_type == "product_view", 1).otherwise(0)),
            add_to_cart_count=sum(when(event.event_type == "add_to_cart", 1).otherwise(0)),
            last_event_at=max(event.occurred_at),
        )
