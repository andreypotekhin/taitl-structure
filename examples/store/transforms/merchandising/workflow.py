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
from examples.store.schemas.product import BlockedProduct, Product
from examples.store.schemas.promotion import Promotion
from examples.store.schemas.taxonomy import ProductTaxonomy, TaxonomyNode
from examples.store.transforms.catalog import PrepareCatalog
from examples.store.transforms.recommender import Recommender
from examples.store.transforms.taxonomy import ExpandProductTaxonomy
from structure import Transform, input, output


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
    preferences = input(UserFeaturePreference)
    feedback_impressions = input(RecommendationImpression, streaming=True)
    feedback_clicks = input(RecommendationClick, streaming=True)
    recommended_products = output(RecommendedProduct)
    recommendation_runs = output(RecommendationRun)
    daily_impressions = output(DailyRecommendationImpressions)
    daily_clicks = output(DailyRecommendationClicks)
    recommendation_signals = output(ProductRecommendationSignal)
    recommendation_purchases = output(RecommendationPurchase)

    catalog = PrepareCatalog(
        products=products,
        blocked_products=blocked_products,
        promotions=promotions,
    )

    taxonomy = ExpandProductTaxonomy(product_taxonomy=product_taxonomy, taxonomy=taxonomy_nodes)

    recommended = Recommender(
        requests=requests,
        catalog=catalog.catalog,
        policy=policy,
        boosts=boosts,
        suppressions=suppressions,
        taxonomy=taxonomy.expanded,
        session_events=session_events,
        fulfilled_orders=fulfilled_orders,
        feedback_impressions=feedback_impressions,
        feedback_clicks=feedback_clicks,
        preferences=preferences,
    )

    result = output(
        recommended_products=recommended.recommended_products,
        recommendation_runs=recommended.recommendation_runs,
        daily_impressions=recommended.daily_impressions,
        daily_clicks=recommended.daily_clicks,
        recommendation_signals=recommended.recommendation_signals,
        recommendation_purchases=recommended.recommendation_purchases,
    )
