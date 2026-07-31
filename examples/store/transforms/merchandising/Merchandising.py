from examples.store.schemas.merchandising import (
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
    ProductRecommendationSignal,
    RecommendationRequest,
    RecommendationRun,
    RecommendedProduct,
)
from examples.store.schemas.product import BlockedProduct, Product
from examples.store.schemas.promotion import Promotion
from examples.store.transforms.merchandising.catalog import PrepareCatalog
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
    signals = input(ProductRecommendationSignal)
    cataloged = stage(
        PrepareCatalog(
            products=products,
            blocked_products=blocked_products,
            promotions=promotions,
        )
    )
    recommended = stage(
        Recommender(
            requests=requests,
            catalog=cataloged.catalog,
            policy=policy,
            boosts=boosts,
            suppressions=suppressions,
            signals=signals,
        )
    )
    recommended_products = output(RecommendedProduct, recommended.recommended_products)
    recommendation_runs = output(RecommendationRun, recommended.recommendation_runs)
