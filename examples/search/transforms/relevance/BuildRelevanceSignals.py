"""Batch relevance snapshots from persisted daily feedback facts."""

from examples.search.schemas.clicks import DailyClicks, DailyImpressions
from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.relevance_signals.build import (
    ContextDailyClicks,
    ContextDailyImpressions,
    DocumentPopularityTotals,
    QueryDocumentSignalTotals,
)
from examples.search.schemas.user import BandFallback, BandMembership, UserBandMembership
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    coalesce,
    datediff,
    exp,
    group_by,
    inner_join,
    left_join,
    log,
    param_join,
    rows_between,
    sum,
    unbounded_following,
    unbounded_preceding,
    union_all,
    when,
    where,
    window,
    window_max,
)
from structure.plugin.pyspark.dsl.expressions import literal


class BuildRelevanceSignals(Transform):
    """Calculate decayed IPS relevance signals for query-document and document grains."""

    daily_impressions = input(DailyImpressions)
    daily_clicks = input(DailyClicks)
    band_memberships = input(BandMembership)
    user_band_memberships = input(UserBandMembership)
    band_fallbacks = input(BandFallback)
    policy = input(RelevancePolicy)
    global_context_impressions = lane(ContextDailyImpressions)
    fallback_context_impressions = lane(ContextDailyImpressions)
    band_context_impressions = lane(ContextDailyImpressions)
    context_impressions = lane(ContextDailyImpressions)
    global_context_clicks = lane(ContextDailyClicks)
    fallback_context_clicks = lane(ContextDailyClicks)
    band_context_clicks = lane(ContextDailyClicks)
    context_clicks = lane(ContextDailyClicks)
    query_signal_totals = lane(QueryDocumentSignalTotals)
    popularity_totals = lane(DocumentPopularityTotals)
    query_document_signals = output(QueryDocumentSignals)
    document_popularity = output(DocumentPopularity)

    @step(input=daily_impressions, output=global_context_impressions)
    def global_impressions(self, impression: DailyImpressions) -> ContextDailyImpressions:
        return ContextDailyImpressions.project(impression)(band_id=literal(None))

    @step(
        input=[daily_impressions, user_band_memberships, band_fallbacks],
        output=fallback_context_impressions,
    )
    def fallback_impressions(
        self, impression: DailyImpressions, membership: UserBandMembership, fallback: BandFallback
    ) -> ContextDailyImpressions:
        inner_join(membership, on=membership.user_id == impression.user_id)
        inner_join(fallback, on=fallback.user_band_id == membership.user_band_id)
        where(membership.user_band_id.is_not_null(), fallback.user_band_fallback_id.is_not_null())
        return ContextDailyImpressions.project(impression)(band_id=fallback.user_band_fallback_id)

    @step(input=[daily_impressions, band_memberships], output=band_context_impressions)
    def band_impressions(self, impression: DailyImpressions, membership: BandMembership) -> ContextDailyImpressions:
        inner_join(membership, on=membership.user_id == impression.user_id)
        where(membership.band_id.is_not_null())
        return ContextDailyImpressions.project(impression)(band_id=membership.user_band_id)

    @step(
        input=[global_context_impressions, fallback_context_impressions, band_context_impressions],
        output=context_impressions,
    )
    def merge_context_impressions(
        self, global_fact: ContextDailyImpressions, fallback_fact: ContextDailyImpressions, band_fact: ContextDailyImpressions
    ) -> ContextDailyImpressions:
        merged = union_all(fallback_fact)
        merged = union_all(band_fact)
        return ContextDailyImpressions.project(merged)

    @step(input=daily_clicks, output=global_context_clicks)
    def global_clicks(self, click: DailyClicks) -> ContextDailyClicks:
        return ContextDailyClicks.project(click)(band_id=literal(None))

    @step(
        input=[daily_clicks, user_band_memberships, band_fallbacks],
        output=fallback_context_clicks,
    )
    def fallback_clicks(
        self, click: DailyClicks, membership: UserBandMembership, fallback: BandFallback
    ) -> ContextDailyClicks:
        inner_join(membership, on=membership.user_id == click.user_id)
        inner_join(fallback, on=fallback.user_band_id == membership.user_band_id)
        where(membership.user_band_id.is_not_null(), fallback.user_band_fallback_id.is_not_null())
        return ContextDailyClicks.project(click)(band_id=fallback.user_band_fallback_id)

    @step(input=[daily_clicks, band_memberships], output=band_context_clicks)
    def band_clicks(self, click: DailyClicks, membership: BandMembership) -> ContextDailyClicks:
        inner_join(membership, on=membership.user_id == click.user_id)
        where(membership.band_id.is_not_null())
        return ContextDailyClicks.project(click)(band_id=membership.user_band_id)

    @step(
        input=[global_context_clicks, fallback_context_clicks, band_context_clicks],
        output=context_clicks,
    )
    def merge_context_clicks(
        self, global_fact: ContextDailyClicks, fallback_fact: ContextDailyClicks, band_fact: ContextDailyClicks
    ) -> ContextDailyClicks:
        merged = union_all(fallback_fact)
        merged = union_all(band_fact)
        return ContextDailyClicks.project(merged)

    @step(input=[context_impressions, context_clicks, policy], output=query_signal_totals)
    def summarize_query(
        self, impression: ContextDailyImpressions, click: ContextDailyClicks, policy: RelevancePolicy
    ) -> QueryDocumentSignalTotals:
        left_join(
            click,
            on=(click.window == impression.window)
            & (click.query == impression.query)
            & (click.document_id == impression.document_id)
            & (click.position == impression.position)
            & (click.examination_propensity == impression.examination_propensity)
            & click.user_id.null_safe_eq(impression.user_id)
            & click.band_id.null_safe_eq(impression.band_id),
        )
        policy = param_join(policy)
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
        group_by(
            band_id=impression.band_id,
            query=impression.query,
            document_id=impression.document_id,
        )
        return QueryDocumentSignalTotals.project(impression)(
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

    @step(input=[context_impressions, context_clicks, policy], output=popularity_totals)
    def summarize_popularity(
        self, impression: ContextDailyImpressions, click: ContextDailyClicks, policy: RelevancePolicy
    ) -> DocumentPopularityTotals:
        left_join(
            click,
            on=(click.window == impression.window)
            & (click.query == impression.query)
            & (click.document_id == impression.document_id)
            & (click.position == impression.position)
            & (click.examination_propensity == impression.examination_propensity)
            & click.user_id.null_safe_eq(impression.user_id)
            & click.band_id.null_safe_eq(impression.band_id),
        )
        policy = param_join(policy)
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
        group_by(band_id=impression.band_id, document_id=impression.document_id)
        return DocumentPopularityTotals.project(impression)(
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
    def normalize_query(self, signal: QueryDocumentSignalTotals, policy: RelevancePolicy) -> QueryDocumentSignals:
        policy = param_join(policy)
        dwell_score = log(1.0 + signal.ips_dwell_credit)
        maximum = window_max(
            dwell_score,
            over=window(
                partition_by=(signal.query, signal.band_id),
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
    def normalize_popularity(self, popularity: DocumentPopularityTotals, policy: RelevancePolicy) -> DocumentPopularity:
        policy = param_join(policy)
        dwell_score = log(1.0 + popularity.ips_dwell_credit)
        maximum = window_max(
            dwell_score,
            over=window(
                partition_by=popularity.band_id,
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
