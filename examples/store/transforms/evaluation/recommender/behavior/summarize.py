from examples.store.schemas.merchandising.evaluation import DailyRecommendationBehavior, RecommendationRequestBehavior
from examples.store.schemas.merchandising.intermediate import (
    DailyRecommendationCounts,
    RecommendationBehaviorImpression,
    RecommendationExposure,
)
from structure import *
from structure.plugin.pyspark import *


class SummarizeRecommendationBehavior(Transform):
    request_behaviors = input(RecommendationRequestBehavior)
    measured_impressions = input(RecommendationBehaviorImpression)
    exposure = lane(RecommendationExposure)
    daily_counts = lane(DailyRecommendationCounts)
    daily_behavior = output(DailyRecommendationBehavior)

    @step(input=measured_impressions, output=exposure)
    def summarize_exposure(self, impression: RecommendationBehaviorImpression) -> RecommendationExposure:
        group_by(
            window=impression.window,
            tenant_id=impression.tenant.tenant_id,
            strategy_id=impression.strategy_id,
            policy_version=impression.policy_version,
        )
        propensity = when(impression.examination_propensity > 0.0, impression.examination_propensity).otherwise(1.0)
        weight = 1.0 / propensity
        return RecommendationExposure.project(impression)(
            exposure_weight=sum(weight),
            click_weight=sum(when(impression.click_count > 0, weight).otherwise(0.0)),
        )

    @step(input=request_behaviors, output=daily_counts)
    def summarize_requests(self, request: RecommendationRequestBehavior) -> DailyRecommendationCounts:
        group_by(
            window=request.window,
            tenant_id=request.tenant.tenant_id,
            strategy_id=request.strategy_id,
            policy_version=request.policy_version,
        )
        request_count = sum(1)
        zero_result_count = sum(when(request.result_count == 0, 1).otherwise(0))
        clicked_request_count = sum(when(request.has_click, 1).otherwise(0))
        return DailyRecommendationCounts.project(request)(
            request_count=request_count,
            zero_result_request_count=zero_result_count,
            clicked_request_count=clicked_request_count,
            zero_result_rate=sum(0.0),
            clicked_request_rate=sum(0.0),
            mean_first_click_rank=avg(request.first_click_rank),
            raw_click_count=sum(request.raw_click_count),
            exposure_adjusted_click_rate=sum(0.0),
        )

    @step(input=[daily_counts, exposure], output=daily_behavior)
    def publish_daily(
        self, daily: DailyRecommendationCounts, exposure: RecommendationExposure
    ) -> DailyRecommendationBehavior:
        left_join(
            exposure,
            on=(exposure.window == daily.window)
            & (exposure.tenant.tenant_id == daily.tenant.tenant_id)
            & (exposure.strategy_id == daily.strategy_id)
            & (exposure.policy_version == daily.policy_version),
        )
        return DailyRecommendationBehavior.project(daily)(
            zero_result_rate=when(
                daily.request_count > 0,
                daily.zero_result_request_count / daily.request_count,
            ).otherwise(None),
            clicked_request_rate=when(
                daily.request_count > 0,
                daily.clicked_request_count / daily.request_count,
            ).otherwise(None),
            exposure_adjusted_click_rate=when(
                exposure.exposure_weight > 0.0,
                exposure.click_weight / exposure.exposure_weight,
            ).otherwise(None),
        )
