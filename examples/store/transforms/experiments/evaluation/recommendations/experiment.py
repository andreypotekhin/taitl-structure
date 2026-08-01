from examples.store.schemas.evaluation import (
    EvaluationBatch,
    RecommendationVariantMetric,
    RecommendationVariantMetricTotals,
)
from examples.store.schemas.experiment import RecommendationExperiment, RecommendationExposure
from examples.store.schemas.merchandising import (
    RecommendationClick,
    RecommendationImpression,
    RecommendationPurchase,
    RecommendationRequest,
    RecommendationRun,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import *


class EvaluateRecommendationExperiment(Transform):
    """Compare observed recommendation behavior by assigned and served variant."""

    evaluation_batch = input(EvaluationBatch)
    experiments = input(RecommendationExperiment)
    exposures = input(RecommendationExposure)
    requests = input(RecommendationRequest)
    runs = input(RecommendationRun)
    impressions = input(RecommendationImpression)
    clicks = input(RecommendationClick)
    purchases = input(RecommendationPurchase)
    totals = lane(RecommendationVariantMetricTotals)
    metrics = output(RecommendationVariantMetric)

    @step(
        input=[requests, evaluation_batch, exposures, experiments, runs, impressions, clicks, purchases], output=totals
    )
    def evaluate(
        self,
        request: RecommendationRequest,
        evaluation_batch: EvaluationBatch,
        exposure: RecommendationExposure,
        experiment: RecommendationExperiment,
        run: RecommendationRun,
        impression: RecommendationImpression,
        click: RecommendationClick,
        purchase: RecommendationPurchase,
    ) -> RecommendationVariantMetricTotals:
        inner_join(
            evaluation_batch,
            on=(request.requested_at >= evaluation_batch.window.start)
            & (request.requested_at < evaluation_batch.window.end),
        )
        left_join(
            purchase,
            on=(purchase.tenant.tenant_id == request.tenant.tenant_id)
            & purchase.request_id.null_safe_eq(request.id),
        )
        inner_join(
            exposure,
            on=(exposure.tenant.tenant_id == request.tenant.tenant_id) & (exposure.request_id == request.id),
        )
        inner_join(
            experiment,
            on=(experiment.tenant.tenant_id == exposure.tenant.tenant_id)
            & (experiment.experiment_id == exposure.experiment_id)
            & (experiment.experiment_version == exposure.experiment_version),
        )
        inner_join(run, on=(run.tenant.tenant_id == request.tenant.tenant_id) & (run.request_id == request.id))
        left_join(impression, on=impression.request_id == request.id)
        left_join(click, on=click.impression_id == impression.id)
        group_by(
            window=evaluation_batch.window,
            tenant_id=request.tenant.tenant_id,
            experiment_id=exposure.experiment_id,
            experiment_version=exposure.experiment_version,
            variant_id=exposure.variant_id,
            maximum_zero_result_rate=experiment.maximum_zero_result_rate,
        )
        request_count = count_distinct(request.id)
        zero_result_count = count_distinct(when(run.result_count == 0, request.id).otherwise(None))
        impression_count = count_distinct(impression.id)
        clicked_count = count_distinct(click.id)
        purchase_count = count_distinct(
            when(purchase.attribution_status == "attributed", purchase.order_id).otherwise(None)
        )
        return RecommendationVariantMetricTotals.project(request)(
            window=evaluation_batch.window,
            experiment_id=exposure.experiment_id,
            experiment_version=exposure.experiment_version,
            variant_id=exposure.variant_id,
            request_count=request_count,
            zero_result_request_count=zero_result_count,
            impression_count=impression_count,
            clicked_request_count=clicked_count,
            attributed_purchase_count=purchase_count,
            maximum_zero_result_rate=experiment.maximum_zero_result_rate,
        )

    @step(input=totals, output=metrics)
    def publish(self, total: RecommendationVariantMetricTotals) -> RecommendationVariantMetric:
        zero_rate = when(total.request_count > 0, total.zero_result_request_count / total.request_count).otherwise(0.0)
        return RecommendationVariantMetric.project(total)(
            zero_result_rate=zero_rate,
            click_through_rate=when(
                total.impression_count > 0, total.clicked_request_count / total.impression_count
            ).otherwise(0.0),
            conversion_rate=when(
                total.request_count > 0, total.attributed_purchase_count / total.request_count
            ).otherwise(0.0),
            zero_result_guardrail_met=when(total.maximum_zero_result_rate.is_null(), None).otherwise(
                zero_rate <= total.maximum_zero_result_rate
            ),
        )
