from examples.store.schemas.merchandising import (
    DailyRecommendationBehavior,
    RecommendationClick,
    RecommendationEvaluationBatch,
    RecommendationImpression,
    RecommendationRequest,
    RecommendationRequestBehavior,
)
from examples.store.transforms.merchandising.clicks.MeasureRecommendationImpressions import (
    MeasureRecommendationImpressions,
)
from examples.store.transforms.merchandising.clicks.MeasureRecommendationRequests import MeasureRecommendationRequests
from examples.store.transforms.merchandising.clicks.SelectEvaluationRequests import SelectEvaluationRequests
from examples.store.transforms.merchandising.clicks.SummarizeRecommendationBehavior import (
    SummarizeRecommendationBehavior,
)
from structure import Transform, input, output, stage


class EvaluateMerchandising(Transform):
    batch = input(RecommendationEvaluationBatch)
    requests = input(RecommendationRequest)
    impressions = input(RecommendationImpression)
    clicks = input(RecommendationClick)

    selected = stage(SelectEvaluationRequests(batch=batch, requests=requests))
    impressions_measured = stage(
        MeasureRecommendationImpressions(
            selected_requests=selected.selected_requests,
            impressions=impressions,
            clicks=clicks,
        )
    )
    requests_measured = stage(
        MeasureRecommendationRequests(
            selected_requests=selected.selected_requests,
            measured_impressions=impressions_measured.measured,
        )
    )
    summarized = stage(
        SummarizeRecommendationBehavior(
            request_behaviors=requests_measured.request_behaviors,
            measured_impressions=impressions_measured.measured,
        )
    )

    request_behaviors = output(RecommendationRequestBehavior, requests_measured.request_behaviors)
    daily_behavior = output(DailyRecommendationBehavior, summarized.daily_behavior)
