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
from examples.store.transforms.recommender.signals.products import BuildProductSignals
from examples.store.transforms.recommender.signals.purchases import BuildPurchaseSignals
from examples.store.transforms.recommender.signals.session import BuildSessionSignals
from structure import Transform, input, output, stage


class BuildSignals(Transform):
    """Build the streaming feedback signals consumed by recommendation serving."""

    session_events = input(SessionEvent, streaming=True)
    fulfilled_orders = input(OrderFulfillment, streaming=True)
    impressions = input(RecommendationImpression, streaming=True)
    clicks = input(RecommendationClick, streaming=True)
    session_features = output(SessionFeature)
    recommendation_purchases = output(RecommendationPurchase)
    daily_impressions = output(DailyRecommendationImpressions)
    daily_clicks = output(DailyRecommendationClicks)
    recommendation_signals = output(ProductRecommendationSignal)

    session = stage(BuildSessionSignals(events=session_events))
    purchases = stage(
        BuildPurchaseSignals(
            fulfilled_orders=fulfilled_orders,
            impressions=impressions,
        )
    )
    recommendation = stage(BuildProductSignals(impressions=impressions, clicks=clicks))
    result = output(
        session_features=session.features,
        recommendation_purchases=purchases.purchases,
        daily_impressions=recommendation.daily_impressions,
        daily_clicks=recommendation.daily_clicks,
        recommendation_signals=recommendation.signals,
    )
