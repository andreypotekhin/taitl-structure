from examples.store.schemas.merchandising import (
    DailyRecommendationBehavior,
    DailyRecommendationClicks,
    DailyRecommendationImpressions,
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
    ProductRecommendationSignal,
    RecommendationClick,
    RecommendationEvaluationBatch,
    RecommendationImpression,
    RecommendationRequest,
    RecommendationRequestBehavior,
    RecommendationRun,
    RecommendedProduct,
)
from examples.store.schemas.product import BlockedProduct, Product
from examples.store.schemas.promotion import Promotion
from examples.store.transforms.merchandising.catalog import PrepareCatalog
from examples.store.transforms.merchandising.clicks import BuildRecommendationSignals, EvaluateMerchandising
from examples.store.transforms.merchandising.recommender import Recommender
from structure import Transform, input, output, stage


class Merchandising(Transform):
    requests = input(RecommendationRequest, streaming=True)
    products = input(Product)
    blocked_products = input(BlockedProduct)
    promotions = input(Promotion)
    policy = input(MerchandisingPolicy)
    boosts = input(MerchandisingBoost)
    suppressions = input(MerchandisingSuppression)
    feedback_impressions = input(RecommendationImpression, streaming=True)
    feedback_clicks = input(RecommendationClick, streaming=True)
    evaluation_batch = input(RecommendationEvaluationBatch)
    evaluation_requests = input(RecommendationRequest)
    evaluation_impressions = input(RecommendationImpression)
    evaluation_clicks = input(RecommendationClick)
    cataloged = stage(
        PrepareCatalog(
            products=products,
            blocked_products=blocked_products,
            promotions=promotions,
        )
    )
    signals_built = stage(
        BuildRecommendationSignals(
            impressions=feedback_impressions,
            clicks=feedback_clicks,
        )
    )
    recommended = stage(
        Recommender(
            requests=requests,
            catalog=cataloged.catalog,
            policy=policy,
            boosts=boosts,
            suppressions=suppressions,
            signals=signals_built.signals,
        )
    )
    evaluated = stage(
        EvaluateMerchandising(
            batch=evaluation_batch,
            requests=evaluation_requests,
            impressions=evaluation_impressions,
            clicks=evaluation_clicks,
        )
    )
    recommended_products = output(RecommendedProduct, recommended.recommended_products)
    recommendation_runs = output(RecommendationRun, recommended.recommendation_runs)
    daily_impressions = output(DailyRecommendationImpressions, signals_built.daily_impressions)
    daily_clicks = output(DailyRecommendationClicks, signals_built.daily_clicks)
    signals = output(ProductRecommendationSignal, signals_built.signals)
    request_behaviors = output(RecommendationRequestBehavior, evaluated.request_behaviors)
    daily_behavior = output(DailyRecommendationBehavior, evaluated.daily_behavior)
