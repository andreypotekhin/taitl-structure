from examples.store.schemas.merchandising import (
    DailyRecommendationClicks,
    DailyRecommendationImpressions,
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
    ProductRecommendationSignal,
    ProductTaxonomy,
    RecommendationClick,
    RecommendationImpression,
    RecommendationPurchase,
    RecommendationRequest,
    RecommendationRun,
    RecommendedProduct,
    SessionEvent,
    TaxonomyNode,
)
from examples.store.schemas.order import OrderFulfillment
from examples.store.schemas.product import BlockedProduct, Product
from examples.store.schemas.promotion import Promotion
from examples.store.transforms.merchandising.catalog import NormalizeCatalog, PrepareCatalog
from examples.store.transforms.merchandising.recommender import Recommender
from examples.store.transforms.merchandising.signals import Signals
from examples.store.transforms.merchandising.taxonomy import ExpandProductTaxonomy
from structure import Transform, input, output, stage


class Merchandising(Transform):
    requests = input(RecommendationRequest, streaming=True)
    products = input(Product)
    blocked_products = input(BlockedProduct)
    promotions = input(Promotion)
    policy = input(MerchandisingPolicy)
    boosts = input(MerchandisingBoost)
    suppressions = input(MerchandisingSuppression)
    product_taxonomy = input(ProductTaxonomy)
    taxonomy_nodes = input(TaxonomyNode)
    session_events = input(SessionEvent, streaming=True)
    fulfilled_orders = input(OrderFulfillment, streaming=True)
    feedback_impressions = input(RecommendationImpression, streaming=True)
    feedback_clicks = input(RecommendationClick, streaming=True)
    recommended_products = output(RecommendedProduct)
    recommendation_runs = output(RecommendationRun)
    daily_impressions = output(DailyRecommendationImpressions)
    daily_clicks = output(DailyRecommendationClicks)
    signals = output(ProductRecommendationSignal)
    recommendation_purchases = output(RecommendationPurchase)

    cataloged = stage(
        PrepareCatalog(
            products=products,
            blocked_products=blocked_products,
            promotions=promotions,
        )
    )
    normalized = stage(NormalizeCatalog(catalog=cataloged.catalog))
    taxonomy_expanded = stage(ExpandProductTaxonomy(product_taxonomy=product_taxonomy, taxonomy=taxonomy_nodes))
    signals_built = stage(
        Signals(
            session_events=session_events,
            fulfilled_orders=fulfilled_orders,
            impressions=feedback_impressions,
            clicks=feedback_clicks,
        )
    )
    recommended = stage(
        Recommender(
            requests=requests,
            catalog=normalized.normalized,
            policy=policy,
            boosts=boosts,
            suppressions=suppressions,
            signals=signals_built.signals,
            taxonomy=taxonomy_expanded.expanded,
            session_features=signals_built.session_features,
        )
    )
    result = output(
        recommended_products=recommended.recommended_products,
        recommendation_runs=recommended.recommendation_runs,
        daily_impressions=signals_built.daily_impressions,
        daily_clicks=signals_built.daily_clicks,
        signals=signals_built.signals,
        recommendation_purchases=signals_built.recommendation_purchases,
    )
