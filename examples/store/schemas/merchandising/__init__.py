from examples.store.schemas.merchandising.catalog import (
    CatalogAvailability,
    CatalogProduct,
    RecommendationCandidate,
    RecommendationCandidateDecision,
)
from examples.store.schemas.merchandising.evaluation import (
    DailyRecommendationBehavior,
    RecommendationEvaluationBatch,
    RecommendationRequestBehavior,
)
from examples.store.schemas.merchandising.feedback import (
    DailyRecommendationClicks,
    DailyRecommendationImpressions,
    ProductRecommendationSignal,
    RecommendationClick,
    RecommendationImpression,
    RecommendationPurchase,
)
from examples.store.schemas.merchandising.session import SessionEvent, SessionFeature
from examples.store.schemas.merchandising.taxonomy import (
    ExpandedProductTaxonomy,
    ProductTaxonomy,
    TaxonomyAncestor,
    TaxonomyNode,
)
from examples.store.schemas.merchandising.intermediate import (
    DailyRecommendationCounts,
    DiversificationDecision,
    DiversifiedRecommendationCandidate,
    ProductRecommendationSignalTotals,
    RankedRecommendationCandidate,
    RecommendationBehaviorImpression,
    RecommendationClickSummary,
    RecommendationExposure,
)
from examples.store.schemas.merchandising.policy import (
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
)
from examples.store.schemas.merchandising.recommendation import (
    RecommendationRequest,
    RecommendationRun,
    RecommendedProduct,
)
