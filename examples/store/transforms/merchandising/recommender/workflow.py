from examples.store.schemas.merchandising import (
    CatalogProduct,
    ExpandedProductTaxonomy,
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
    ProductRecommendationSignal,
    RecommendationRequest,
    RecommendationRun,
    RecommendedProduct,
    SessionFeature,
)
from examples.store.transforms.merchandising.ranking import Ranker
from examples.store.transforms.merchandising.recommender.admit import SelectRecommendationCandidates
from examples.store.transforms.merchandising.recommender.diversify import DiversifyRecommendations
from examples.store.transforms.merchandising.recommender.filter import FilterRecommendationCandidates
from examples.store.transforms.merchandising.recommender.generate import GenerateRecommendationCandidates
from examples.store.transforms.merchandising.recommender.publish import SelectRecommendedProducts
from examples.store.transforms.merchandising.recommender.rank import RankRecommendationCandidates
from examples.store.transforms.merchandising.recommender.summarize import SummarizeRecommendationRuns
from structure import Transform, input, output, parameter, stage


class Recommender(Transform):
    ranker = parameter(Ranker())

    requests = input(RecommendationRequest, streaming=True)
    catalog = input(CatalogProduct)
    policy = input(MerchandisingPolicy)
    boosts = input(MerchandisingBoost)
    suppressions = input(MerchandisingSuppression)
    signals = input(ProductRecommendationSignal)
    taxonomy = input(ExpandedProductTaxonomy)
    session_features = input(SessionFeature)
    recommended_products = output(RecommendedProduct)
    recommendation_runs = output(RecommendationRun)

    selected = stage(
        SelectRecommendationCandidates(
            requests=requests,
            catalog=catalog,
        )
    )
    retrieved = stage(
        GenerateRecommendationCandidates(
            requests=requests,
            catalog=catalog,
            taxonomy=taxonomy,
            session_features=session_features,
            signals=signals,
        )
    )
    filtered = stage(
        FilterRecommendationCandidates(
            candidates=retrieved.candidates,
            suppressions=suppressions,
            session_features=session_features,
        )
    )
    ranked = stage(
        RankRecommendationCandidates(
            candidates=filtered.filtered,
            policy=policy,
            boosts=boosts,
            suppressions=suppressions,
            signals=signals,
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
    )
