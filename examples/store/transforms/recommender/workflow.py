from examples.store.schemas.catalog import CatalogProduct
from examples.store.schemas.merchandising import (
    DailyRecommendationClicks,
    DailyRecommendationImpressions,
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
    ProductRecommendationSignal,
    RecommendationClick,
    RecommendationImpression,
    RecommendationPurchase,
    RecommendationRequest,
    RecommendationRun,
    RecommendedProduct,
    SessionEvent,
)
from examples.store.schemas.order import OrderFulfillment
from examples.store.schemas.personalization import UserFeaturePreference
from examples.store.schemas.taxonomy import ExpandedProductTaxonomy
from examples.store.transforms.personalization import BuildPersonalizedRecommendations
from examples.store.transforms.recommender.candidates import BuildRecommendationCandidates
from examples.store.transforms.recommender.diversify import DiversifyRecommendations
from examples.store.transforms.recommender.publish import SelectRecommendedProducts
from examples.store.transforms.recommender.ranking import Ranker, RankRecommendationCandidates
from examples.store.transforms.recommender.signals import BuildRecommendationSignals
from examples.store.transforms.recommender.summarize import SummarizeRecommendationRuns
from structure import *


@transform
class Recommender(Transform):
    ranker = parameter(Ranker())

    requests = input(RecommendationRequest, streaming=True)
    catalog = input(CatalogProduct)
    policy = input(MerchandisingPolicy)
    boosts = input(MerchandisingBoost)
    suppressions = input(MerchandisingSuppression)
    taxonomy = input(ExpandedProductTaxonomy)
    session_events = input(SessionEvent, streaming=True)
    fulfilled_orders = input(OrderFulfillment, streaming=True)
    preferences = input(UserFeaturePreference)
    feedback_impressions = input(RecommendationImpression, streaming=True)
    feedback_clicks = input(RecommendationClick, streaming=True)
    recommended_products = output(RecommendedProduct)
    recommendation_runs = output(RecommendationRun)
    daily_impressions = output(DailyRecommendationImpressions)
    daily_clicks = output(DailyRecommendationClicks)
    recommendation_signals = output(ProductRecommendationSignal)
    recommendation_purchases = output(RecommendationPurchase)

    signals = stage(
        BuildRecommendationSignals(
            session_events=session_events,
            fulfilled_orders=fulfilled_orders,
            impressions=feedback_impressions,
            clicks=feedback_clicks,
        )
    )
    personalized = stage(
        BuildPersonalizedRecommendations(
            requests=requests,
            catalog=catalog,
            preferences=preferences,
            session_events=session_events,
            fulfilled_orders=fulfilled_orders,
        )
    )
    candidates = stage(
        BuildRecommendationCandidates(
            requests=requests,
            catalog=catalog,
            taxonomy=taxonomy,
            session_features=signals.session_features,
            signals=signals.recommendation_signals,
            suppressions=suppressions,
        )
    )
    ranked = stage(
        RankRecommendationCandidates(
            candidates=candidates.candidates,
            policy=policy,
            boosts=boosts,
            suppressions=suppressions,
            signals=signals.recommendation_signals,
            personalized=personalized.recommendations,
            ranker=ranker,
        )
    )
    diversified = stage(
        DiversifyRecommendations(
            ranked=ranked.ranked_candidates,
            policy=policy,
        )
    )
    published = stage(SelectRecommendedProducts(ranked_candidates=diversified.diversified))
    summarized = stage(
        SummarizeRecommendationRuns(
            requests=requests,
            policy=policy,
            products=published.products,
        )
    )
    result = output(
        recommended_products=published.products,
        recommendation_runs=summarized.runs,
        daily_impressions=signals.daily_impressions,
        daily_clicks=signals.daily_clicks,
        recommendation_signals=signals.recommendation_signals,
        recommendation_purchases=signals.recommendation_purchases,
    )
