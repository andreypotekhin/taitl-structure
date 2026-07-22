"""Batch relevance snapshots from persisted daily feedback facts."""

from examples.search.schemas.clicks import DailyClicks, DailyImpressions
from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    coalesce,
    cross_join,
    datediff,
    exp,
    group_by,
    left_join,
    log,
    rows_between,
    sum,
    unbounded_following,
    unbounded_preceding,
    when,
    window,
    window_max,
)


class BuildRelevanceSignals(Transform):
    """Calculate decayed IPS relevance signals for query-document and document grains."""

    daily_impressions = input(DailyImpressions)
    daily_clicks = input(DailyClicks)
    policy = input(RelevancePolicy)
    query_signal_totals = lane(QueryDocumentSignals)
    popularity_totals = lane(DocumentPopularity)
    query_document_signals = output(QueryDocumentSignals)
    document_popularity = output(DocumentPopularity)

    @step(input=[daily_impressions, daily_clicks, policy], output=query_signal_totals)
    def summarize_query(
        self, impression: DailyImpressions, click: DailyClicks, policy: RelevancePolicy
    ) -> QueryDocumentSignals:
        left_join(
            click,
            on=(click.window == impression.window)
            & (click.query == impression.query)
            & (click.document_id == impression.document_id)
            & (click.position == impression.position)
            & (click.examination_propensity == impression.examination_propensity),
        )
        policy = cross_join(policy, allow_cartesian=True)
        clicks = coalesce(click.click_count, 0)
        clicked_impressions = coalesce(click.clicked_impression_count, 0)
        dwell_seconds = coalesce(click.dwell_seconds, 0.0)
        dwell_credit = coalesce(click.dwell_credit, 0.0)
        long_clicks = coalesce(click.long_click_count, 0)
        age_days = when(
            datediff(policy.evaluated_at, impression.window.end) > 0,
            datediff(policy.evaluated_at, impression.window.end),
        ).otherwise(0)
        decay = exp(-log(2.0) * age_days / policy.half_life_days)
        group_by(query=impression.query, document_id=impression.document_id)
        return QueryDocumentSignals.base(impression)(
            impression_count=sum(impression.impression_count),
            click_count=sum(clicks),
            clicked_impression_count=sum(clicked_impressions),
            dwell_seconds=sum(dwell_seconds),
            long_click_count=sum(long_clicks),
            click_through_rate=sum(0.0),
            ips_clicks=sum(clicks * decay / impression.examination_propensity),
            ips_dwell_credit=sum(dwell_credit * decay / impression.examination_propensity),
            ips_click_through_rate=sum(0.0),
            ips_impression_weight=sum(impression.impression_count * decay / impression.examination_propensity),
            ips_clicked_impression_weight=sum(clicked_impressions * decay / impression.examination_propensity),
            normalized_dwell_score=sum(0.0),
            normalized_ctr_score=sum(0.0),
            normalized_score=sum(0.0),
        )

    @step(input=[daily_impressions, daily_clicks, policy], output=popularity_totals)
    def summarize_popularity(
        self, impression: DailyImpressions, click: DailyClicks, policy: RelevancePolicy
    ) -> DocumentPopularity:
        left_join(
            click,
            on=(click.window == impression.window)
            & (click.query == impression.query)
            & (click.document_id == impression.document_id)
            & (click.position == impression.position)
            & (click.examination_propensity == impression.examination_propensity),
        )
        policy = cross_join(policy, allow_cartesian=True)
        clicks = coalesce(click.click_count, 0)
        clicked_impressions = coalesce(click.clicked_impression_count, 0)
        dwell_seconds = coalesce(click.dwell_seconds, 0.0)
        dwell_credit = coalesce(click.dwell_credit, 0.0)
        long_clicks = coalesce(click.long_click_count, 0)
        age_days = when(
            datediff(policy.evaluated_at, impression.window.end) > 0,
            datediff(policy.evaluated_at, impression.window.end),
        ).otherwise(0)
        decay = exp(-log(2.0) * age_days / policy.half_life_days)
        group_by(document_id=impression.document_id)
        return DocumentPopularity.base(impression)(
            impression_count=sum(impression.impression_count),
            click_count=sum(clicks),
            clicked_impression_count=sum(clicked_impressions),
            dwell_seconds=sum(dwell_seconds),
            long_click_count=sum(long_clicks),
            click_through_rate=sum(0.0),
            ips_clicks=sum(clicks * decay / impression.examination_propensity),
            ips_dwell_credit=sum(dwell_credit * decay / impression.examination_propensity),
            ips_click_through_rate=sum(0.0),
            ips_impression_weight=sum(impression.impression_count * decay / impression.examination_propensity),
            ips_clicked_impression_weight=sum(clicked_impressions * decay / impression.examination_propensity),
            normalized_dwell_score=sum(0.0),
            normalized_ctr_score=sum(0.0),
            normalized_score=sum(0.0),
        )

    @step(input=[query_signal_totals, policy], output=query_document_signals)
    def normalize_query(self, signal: QueryDocumentSignals, policy: RelevancePolicy) -> QueryDocumentSignals:
        policy = cross_join(policy, allow_cartesian=True)
        dwell_score = log(1.0 + signal.ips_dwell_credit)
        maximum = window_max(
            dwell_score,
            over=window(
                partition_by=signal.query,
                order_by=signal.document_id,
                frame=rows_between(unbounded_preceding(), unbounded_following()),
            ),
        )
        normalized_dwell_score = when(maximum > 0.0, dwell_score / maximum).otherwise(0.0)
        ips_ctr = signal.ips_clicked_impression_weight / signal.ips_impression_weight
        normalized_ctr_score = when(
            signal.impression_count >= policy.minimum_ctr_impressions,
            ips_ctr,
        ).otherwise(0.0)
        return QueryDocumentSignals.project(signal)(
            click_through_rate=signal.clicked_impression_count / signal.impression_count,
            ips_click_through_rate=ips_ctr,
            normalized_dwell_score=normalized_dwell_score,
            normalized_ctr_score=normalized_ctr_score,
            normalized_score=policy.dwell_feedback_weight * normalized_dwell_score
            + policy.ctr_feedback_weight * normalized_ctr_score,
        )

    @step(input=[popularity_totals, policy], output=document_popularity)
    def normalize_popularity(self, popularity: DocumentPopularity, policy: RelevancePolicy) -> DocumentPopularity:
        policy = cross_join(policy, allow_cartesian=True)
        dwell_score = log(1.0 + popularity.ips_dwell_credit)
        maximum = window_max(
            dwell_score,
            over=window(
                partition_by="all documents",
                order_by=popularity.document_id,
                frame=rows_between(unbounded_preceding(), unbounded_following()),
            ),
        )
        normalized_dwell_score = when(maximum > 0.0, dwell_score / maximum).otherwise(0.0)
        ips_ctr = popularity.ips_clicked_impression_weight / popularity.ips_impression_weight
        normalized_ctr_score = when(
            popularity.impression_count >= policy.minimum_ctr_impressions,
            ips_ctr,
        ).otherwise(0.0)
        return DocumentPopularity.project(popularity)(
            click_through_rate=popularity.clicked_impression_count / popularity.impression_count,
            ips_click_through_rate=ips_ctr,
            normalized_dwell_score=normalized_dwell_score,
            normalized_ctr_score=normalized_ctr_score,
            normalized_score=policy.dwell_feedback_weight * normalized_dwell_score
            + policy.ctr_feedback_weight * normalized_ctr_score,
        )
