from examples.store.schemas.merchandising import RecommendationEvaluationBatch, RecommendationRequest
from examples.store.schemas.merchandising.evaluation import RecommendationRequestBehavior
from structure import *
from structure.plugin.pyspark import *


class SelectEvaluationRequests(Transform):
    batch = input(RecommendationEvaluationBatch)
    requests = input(RecommendationRequest)
    selected_requests = output(RecommendationRequestBehavior)

    @step(input=[requests, batch], output=selected_requests)
    def select_requests(
        self, request: RecommendationRequest, batch: RecommendationEvaluationBatch
    ) -> RecommendationRequestBehavior:
        cross_join(batch, allow_cartesian=True)
        where((request.requested_at >= batch.window.start) & (request.requested_at < batch.window.end))
        return RecommendationRequestBehavior.project(request)(
            window=batch.window,
            request_id=request.id,
            result_count=0,
            clicked_result_count=0,
            has_click=False,
            first_click_rank=None,
            raw_click_count=0,
        )
