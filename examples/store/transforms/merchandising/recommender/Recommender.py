from examples.store.schemas.merchandising import (
    CatalogProduct,
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
    ProductRecommendationSignal,
    RecommendationRequest,
    RecommendationRun,
    RecommendedProduct,
)
from examples.store.transforms.merchandising.recommender.admit import (
    SelectRecommendationCandidates,
    SelectRecommendedProducts,
)
from examples.store.transforms.merchandising.recommender.rank import RankRecommendationCandidates
from examples.store.transforms.merchandising.recommender.summarize import SummarizeRecommendationRuns
from structure import Transform, input, output, stage


class Recommender(Transform):
    requests = input(RecommendationRequest, streaming=True)
    catalog = input(CatalogProduct)
    policy = input(MerchandisingPolicy)
    boosts = input(MerchandisingBoost)
    suppressions = input(MerchandisingSuppression)
    signals = input(ProductRecommendationSignal)
    selected = stage(
        SelectRecommendationCandidates(
            requests=requests,
            catalog=catalog,
        )
    )
    ranked = stage(
        RankRecommendationCandidates(
            candidates=selected.candidates,
            policy=policy,
            boosts=boosts,
            suppressions=suppressions,
            signals=signals,
        )
    )
    admitted = stage(SelectRecommendedProducts(ranked_candidates=ranked.ranked_candidates))
    summarized = stage(
        SummarizeRecommendationRuns(
            requests=requests,
            policy=policy,
            products=admitted.products,
        )
    )
    recommended_products = output(RecommendedProduct, admitted.products)
    recommendation_runs = output(RecommendationRun, summarized.runs)
