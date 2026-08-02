from examples.store.schemas.catalog import CatalogProduct
from examples.store.schemas.merchandising import (
    MerchandisingSuppression,
    ProductRecommendationSignal,
    RecommendationCandidate,
    RecommendationCandidateDecision,
    RecommendationRequest,
    SessionFeature,
)
from examples.store.schemas.taxonomy import ExpandedProductTaxonomy
from examples.store.transforms.recommender.candidates.admit import SelectRecommendationCandidates
from examples.store.transforms.recommender.candidates.filter import FilterRecommendationCandidates
from examples.store.transforms.recommender.candidates.generate import GenerateRecommendationCandidates
from structure import Transform, input, output, stage, transform


@transform
class BuildRecommendationCandidates(Transform):
    """Admit, generate, and filter recommendation candidates."""

    requests = input(RecommendationRequest, streaming=True)
    catalog = input(CatalogProduct)
    taxonomy = input(ExpandedProductTaxonomy)
    session_features = input(SessionFeature)
    signals = input(ProductRecommendationSignal)
    suppressions = input(MerchandisingSuppression)
    candidates = output(RecommendationCandidate)
    decisions = output(RecommendationCandidateDecision)

    admitted = stage(
        SelectRecommendationCandidates(
            requests=requests,
            catalog=catalog,
        )
    )
    retrieved = stage(
        GenerateRecommendationCandidates(
            admitted=admitted.candidates,
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
    result = output(
        candidates=filtered.filtered,
        decisions=filtered.decisions,
    )
