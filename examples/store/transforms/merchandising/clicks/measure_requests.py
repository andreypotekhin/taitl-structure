from examples.store.schemas.merchandising.evaluation import RecommendationRequestBehavior
from examples.store.schemas.merchandising.intermediate import RecommendationBehaviorImpression
from structure import *
from structure.plugin.pyspark import *


class MeasureRecommendationRequests(Transform):
    selected_requests = input(RecommendationRequestBehavior)
    measured_impressions = input(RecommendationBehaviorImpression)
    request_behaviors = output(RecommendationRequestBehavior)

    @step(input=[selected_requests, measured_impressions], output=request_behaviors)
    def measure_requests(
        self, request: RecommendationRequestBehavior, impression: RecommendationBehaviorImpression
    ) -> RecommendationRequestBehavior:
        left_join(
            impression,
            on=(impression.tenant.tenant_id == request.tenant.tenant_id)
            & (impression.request_id == request.request_id),
        )
        group_by(
            window=request.window,
            tenant_id=request.tenant.tenant_id,
            request_id=request.request_id,
            strategy_id=request.strategy_id,
            policy_version=request.policy_version,
        )
        result_count = sum(when(impression.impression_id.is_not_null(), 1).otherwise(0))
        clicked_result_count = sum(when(impression.click_count > 0, 1).otherwise(0))
        return RecommendationRequestBehavior(
            window=request.window,
            tenant=request.tenant,
            request_id=request.request_id,
            strategy_id=request.strategy_id,
            policy_version=request.policy_version,
            result_count=result_count,
            clicked_result_count=clicked_result_count,
            has_click=bool_or(coalesce(impression.click_count, 0) > 0),
            first_click_rank=min(impression.rank, where=impression.click_count > 0),
            raw_click_count=sum(coalesce(impression.click_count, 0)),
        )
