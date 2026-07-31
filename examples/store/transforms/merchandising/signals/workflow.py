from examples.store.schemas.merchandising import (
    DailyRecommendationClicks,
    DailyRecommendationImpressions,
    ProductRecommendationSignal,
    RecommendationClick,
    RecommendationImpression,
    RecommendationPurchase,
    SessionEvent,
    SessionFeature,
)
from examples.store.schemas.order import OrderFulfillment
from examples.store.transforms.merchandising.signals.build_signals import BuildRecommendationSignals
from examples.store.transforms.merchandising.signals.purchases import BuildRecommendationPurchaseSignals
from examples.store.transforms.merchandising.signals.session import BuildSessionSignals
from structure import Transform, input, output, stage


class Signals(Transform):
    """Build the streaming feedback signals consumed by merchandising."""

    session_events = input(SessionEvent, streaming=True)
    fulfilled_orders = input(OrderFulfillment, streaming=True)
    impressions = input(RecommendationImpression, streaming=True)
    clicks = input(RecommendationClick, streaming=True)
    session_features = output(SessionFeature)
    recommendation_purchases = output(RecommendationPurchase)
    daily_impressions = output(DailyRecommendationImpressions)
    daily_clicks = output(DailyRecommendationClicks)
    signals = output(ProductRecommendationSignal)

    sessionized = stage(BuildSessionSignals(events=session_events))
    purchases = stage(
        BuildRecommendationPurchaseSignals(
            fulfilled_orders=fulfilled_orders,
            impressions=impressions,
        )
    )
    recommendation = stage(BuildRecommendationSignals(impressions=impressions, clicks=clicks))
    result = output(
        session_features=sessionized.features,
        recommendation_purchases=purchases.purchases,
        daily_impressions=recommendation.daily_impressions,
        daily_clicks=recommendation.daily_clicks,
        signals=recommendation.signals,
    )
