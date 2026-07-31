from examples.store.schemas.merchandising import (
    DailyRecommendationBehavior,
    RecommendationClick,
    RecommendationEvaluationBatch,
    RecommendationImpression,
    RecommendationRequest,
    RecommendationRequestBehavior,
)
from examples.store.transforms.evaluation.recommender.behavior.admit import SelectEvaluationRequests
from examples.store.transforms.evaluation.recommender.behavior.measure_impressions import (
    MeasureRecommendationImpressions,
)
from examples.store.transforms.evaluation.recommender.behavior.measure_requests import MeasureRecommendationRequests
from examples.store.transforms.evaluation.recommender.behavior.summarize import SummarizeRecommendationBehavior
from structure import Transform, input, output, stage


class EvaluateRecommendations(Transform):
    """Evaluate recommendation behavior independently from recommendation serving."""

    batch = input(RecommendationEvaluationBatch)
    requests = input(RecommendationRequest)
    impressions = input(RecommendationImpression)
    clicks = input(RecommendationClick)
    request_behaviors = output(RecommendationRequestBehavior)
    daily_behavior = output(DailyRecommendationBehavior)

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
    result = output(
        request_behaviors=requests_measured.request_behaviors,
        daily_behavior=summarized.daily_behavior,
    )
