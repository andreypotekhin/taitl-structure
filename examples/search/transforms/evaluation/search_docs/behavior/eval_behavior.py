"""Daily request-aware document-search behavior evaluation."""

from examples.search.schemas.clicks import Click, Impression, SearchRequest
from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.behavior import (
    BehaviorDailyCounts,
    BehaviorExposure,
    BehaviorImpression,
    BehaviorRequest,
    BehaviorRequestMetrics,
    BehaviorRequestTotals,
    DailyDocumentSearchBehavior,
    DocumentSearchRequestBehavior,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    avg,
    bool_or,
    coalesce,
    cross_join,
    event_time_between,
    group_by,
    inner_join,
    left_join,
    min,
    sum,
    when,
    where,
)


class EvaluateDocumentSearchBehavior(Transform):
    """Summarize observed click satisfaction for each served result list."""

    batch = input(EvaluationBatch)
    requests = input(SearchRequest)
    selected_requests = lane(BehaviorRequest)
    impressions = input(Impression)
    clicks = input(Click)
    displayed = lane(BehaviorImpression)
    clicked = lane(BehaviorImpression)
    measured = lane(BehaviorImpression)
    request_totals = lane(BehaviorRequestTotals)
    request_metrics = lane(BehaviorRequestMetrics)
    measured_requests = lane(DocumentSearchRequestBehavior)
    exposure = lane(BehaviorExposure)
    daily_counts = lane(BehaviorDailyCounts)
    summarized_daily = lane(DailyDocumentSearchBehavior)
    request_behaviors = output(DocumentSearchRequestBehavior)
    daily_behavior = output(DailyDocumentSearchBehavior)

    @step(output=selected_requests)
    def select_requests(self, request: SearchRequest, batch: EvaluationBatch) -> BehaviorRequest:
        cross_join(batch, allow_cartesian=True)
        where((request.requested_at >= batch.window.start) & (request.requested_at < batch.window.end))
        return BehaviorRequest(
            window=batch.window,
            params=None,
            experiment_id=request.experiment_id,
            band_id=None,
            search_request_id=request.id,
            ranking_version=request.ranking_version,
            query=request.query,
        )

    @step(output=displayed)
    def select_impressions(self, request: BehaviorRequest, impression: Impression) -> BehaviorImpression:
        inner_join(on=impression.search_request_id == request.search_request_id)
        return BehaviorImpression.project(request, impression)(
            search_request_id=request.search_request_id,
            query=request.query,
            impression_id=impression.id,
            click_count=0,
            long_click_count=0,
            dwell_credit=0.0,
        )

    @step(output=clicked)
    def attribute_clicks(self, impression: BehaviorImpression, click: Click) -> BehaviorImpression:
        inner_join(
            on=(click.impression_id == impression.impression_id)
            & event_time_between(impression.shown_at, click.occurred_at, upper="24 hours")
        )
        dwell = when(click.dwell_seconds > 0.0, click.dwell_seconds).otherwise(0.0)
        group_by(
            window=impression.window,
            params=impression.params,
            experiment_id=impression.experiment_id,
            band_id=impression.band_id,
            search_request_id=impression.search_request_id,
            ranking_version=impression.ranking_version,
            query=impression.query,
            impression_id=impression.impression_id,
            shown_at=impression.shown_at,
            document_id=impression.document_id,
            position=impression.position,
            examination_propensity=impression.examination_propensity,
        )
        return BehaviorImpression.project(impression)(
            click_count=sum(1),
            long_click_count=sum(when(dwell >= 10.0, 1).otherwise(0)),
            dwell_credit=sum(when(dwell < 60.0, dwell).otherwise(60.0) / 60.0),
        )

    @step(output=measured)
    def measure_impressions(self, displayed: BehaviorImpression, clicked: BehaviorImpression) -> BehaviorImpression:
        left_join(on=clicked.impression_id == displayed.impression_id)
        return BehaviorImpression.project(displayed)(
            click_count=coalesce(clicked.click_count, 0),
            long_click_count=coalesce(clicked.long_click_count, 0),
            dwell_credit=coalesce(clicked.dwell_credit, 0.0),
        )

    @step(
        input=[selected_requests, measured],
        output=request_totals,
    )
    def measure_requests(self, selected: BehaviorRequest, measured: BehaviorImpression) -> BehaviorRequestTotals:
        left_join(on=measured.search_request_id == selected.search_request_id)
        group_by(
            window=selected.window,
            params=selected.params,
            experiment_id=selected.experiment_id,
            band_id=selected.band_id,
            search_request_id=selected.search_request_id,
            ranking_version=selected.ranking_version,
            query=selected.query,
        )
        result_count = sum(when(measured.impression_id.is_not_null(), 1).otherwise(0))
        clicked_count = sum(when(measured.click_count > 0, 1).otherwise(0))
        long_count = sum(when(measured.long_click_count > 0, 1).otherwise(0))
        first_long = min(measured.position, where=measured.long_click_count > 0)
        return BehaviorRequestTotals.project(selected)(
            result_count=result_count,
            clicked_result_count=clicked_count,
            long_clicked_result_count=long_count,
            has_click=bool_or(measured.click_count > 0),
            has_long_click=bool_or(measured.long_click_count > 0),
            first_click_rank=min(measured.position, where=measured.click_count > 0),
            first_long_click_rank=first_long,
            reciprocal_first_long_click_rank=sum(0.0),
            raw_click_count=sum(coalesce(measured.click_count, 0)),
            raw_long_click_count=sum(coalesce(measured.long_click_count, 0)),
        )

    @step(input=request_totals, output=request_metrics)
    def calculate_reciprocal_rank(self, request: BehaviorRequestTotals) -> BehaviorRequestMetrics:
        return BehaviorRequestMetrics.project(request)(
            reciprocal_first_long_click_rank=coalesce(
                when(request.first_long_click_rank.is_not_null(), 1.0 / request.first_long_click_rank).otherwise(0.0),
                0.0,
            )
        )

    @step(input=request_metrics, output=measured_requests)
    def publish_requests(self, request: BehaviorRequestMetrics) -> DocumentSearchRequestBehavior:
        return DocumentSearchRequestBehavior.project(request)

    @step(input=measured, output=exposure)
    def summarize_exposure(self, measured: BehaviorImpression) -> BehaviorExposure:
        group_by(
            window=measured.window,
            params=measured.params,
            experiment_id=measured.experiment_id,
            band_id=measured.band_id,
            ranking_version=measured.ranking_version,
        )
        weight = 1.0 / measured.examination_propensity
        return BehaviorExposure(
            window=measured.window,
            params=measured.params,
            experiment_id=measured.experiment_id,
            band_id=measured.band_id,
            ranking_version=measured.ranking_version,
            ips_impression_weight=sum(weight),
            ips_long_click_weight=sum(when(measured.long_click_count > 0, weight).otherwise(0.0)),
            ips_dwell_credit=sum(measured.dwell_credit * weight),
        )

    @step(input=request_metrics, output=daily_counts)
    def summarize_requests(self, request: BehaviorRequestMetrics) -> BehaviorDailyCounts:
        group_by(
            window=request.window,
            params=request.params,
            experiment_id=request.experiment_id,
            band_id=request.band_id,
            ranking_version=request.ranking_version,
        )
        return BehaviorDailyCounts.project(request)(
            request_count=sum(1),
            zero_result_request_count=sum(when(request.result_count == 0, 1).otherwise(0)),
            clicked_request_count=sum(when(request.has_click, 1).otherwise(0)),
            long_clicked_request_count=sum(when(request.has_long_click, 1).otherwise(0)),
            no_click_request_count=sum(when(~request.has_click, 1).otherwise(0)),
            no_long_click_request_count=sum(when(~request.has_long_click, 1).otherwise(0)),
            raw_click_count=sum(request.raw_click_count),
            raw_long_click_count=sum(request.raw_long_click_count),
            mean_first_click_rank=avg(request.first_click_rank),
            mean_first_long_click_rank=avg(request.first_long_click_rank),
            mean_reciprocal_first_long_click_rank=avg(request.reciprocal_first_long_click_rank),
            ips_long_click_rate=sum(0.0),
            ips_dwell_credit_per_impression=sum(0.0),
        )

    @step(input=[daily_counts, exposure], output=summarized_daily)
    def publish_daily(self, daily: BehaviorDailyCounts, exposure: BehaviorExposure) -> DailyDocumentSearchBehavior:
        left_join(
            on=(exposure.window == daily.window)
            & exposure.params.null_safe_eq(daily.params)
            & exposure.experiment_id.null_safe_eq(daily.experiment_id)
            & (exposure.ranking_version == daily.ranking_version)
        )
        return DailyDocumentSearchBehavior.project(daily)(
            ips_long_click_rate=when(
                exposure.ips_impression_weight > 0.0,
                exposure.ips_long_click_weight / exposure.ips_impression_weight,
            ).otherwise(None),
            ips_dwell_credit_per_impression=when(
                exposure.ips_impression_weight > 0.0,
                exposure.ips_dwell_credit / exposure.ips_impression_weight,
            ).otherwise(None),
        )

    @step(input=measured_requests, output=request_behaviors)
    def publish_request_behaviors(self, request: DocumentSearchRequestBehavior) -> DocumentSearchRequestBehavior:
        return DocumentSearchRequestBehavior.project(request)

    @step(input=summarized_daily, output=daily_behavior)
    def publish_daily_behavior(self, daily: DailyDocumentSearchBehavior) -> DailyDocumentSearchBehavior:
        return DailyDocumentSearchBehavior.project(daily)
