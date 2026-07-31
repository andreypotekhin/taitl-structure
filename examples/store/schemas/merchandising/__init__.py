from examples.store.schemas.merchandising.catalog import (
    CatalogAvailability,
    CatalogProduct,
    RecommendationCandidate,
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
)
from examples.store.schemas.merchandising.intermediate import (
    DailyRecommendationCounts,
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
