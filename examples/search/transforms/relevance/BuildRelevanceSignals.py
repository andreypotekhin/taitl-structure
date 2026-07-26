"""Batch relevance snapshots from persisted daily feedback facts."""

from examples.search.schemas.clicks import DailyClicks, DailyImpressions
from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
from examples.search.schemas.user import BandFallback, UserBand
from structure import Transform, input, lane, output, raw, step
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
    user_bands = input(UserBand)
    band_fallbacks = input(BandFallback)
    policy = input(RelevancePolicy)
    context_impressions = lane(DailyImpressions)
    context_clicks = lane(DailyClicks)
    query_signal_totals = lane(QueryDocumentSignals)
    popularity_totals = lane(DocumentPopularity)
    query_document_signals = output(QueryDocumentSignals)
    document_popularity = output(DocumentPopularity)

    @step(input=daily_impressions, output=context_impressions)
    def declare_context_impressions(self, impression: DailyImpressions) -> DailyImpressions:
        """Declare the context-expanded exposure lane before its hierarchy hook replaces it."""

        return DailyImpressions(
            window=impression.window,
            query=impression.query,
            document_id=impression.document_id,
            position=impression.position,
            examination_propensity=impression.examination_propensity,
            user_id=impression.user_id,
            band_id=None,
            impression_count=impression.impression_count,
        )

    @raw(
        input=[input(daily_impressions), input(user_bands), input(band_fallbacks)],
        output=context_impressions,
    )
    def expand_impressions(self, *, daily_impressions, user_bands, band_fallbacks, context_impressions, spark, ctx):
        """Duplicate logged-in facts into their non-global fallback contexts.

        Global rows stay single and scoped rows are expanded only here, keeping all
        decay and metric calculations in the ordinary typed steps below.
        """

        from pyspark.sql import functions as F

        global_facts = daily_impressions.withColumn("band_id", F.lit(None).cast("string"))
        fallback_candidates = band_fallbacks.where(F.col("fallback_band_id").isNotNull()).select(
            F.col("band_id").alias("source_band_id"),
            "fallback_band_id",
            "ordinal",
        )
        scoped = daily_impressions.drop("band_id").join(user_bands, "user_id", "left").join(
            fallback_candidates,
            user_bands.band_id == fallback_candidates.source_band_id,
            "inner",
        ).drop("band_id", "source_band_id").withColumnRenamed("fallback_band_id", "band_id").drop("ordinal")
        return global_facts.unionByName(scoped, allowMissingColumns=False)

    @step(input=daily_clicks, output=context_clicks)
    def declare_context_clicks(self, click: DailyClicks) -> DailyClicks:
        """Declare the context-expanded click lane before its hierarchy hook replaces it."""

        return DailyClicks(
            window=click.window,
            query=click.query,
            document_id=click.document_id,
            position=click.position,
            examination_propensity=click.examination_propensity,
            user_id=click.user_id,
            band_id=None,
            click_count=click.click_count,
            clicked_impression_count=click.clicked_impression_count,
            dwell_seconds=click.dwell_seconds,
            dwell_credit=click.dwell_credit,
            long_click_count=click.long_click_count,
        )

    @raw(input=[input(daily_clicks), input(user_bands), input(band_fallbacks)], output=context_clicks)
    def expand_clicks(self, *, daily_clicks, user_bands, band_fallbacks, context_clicks, spark, ctx):
        """Duplicate click facts into the same non-global fallback contexts as exposures."""

        from pyspark.sql import functions as F

        global_facts = daily_clicks.withColumn("band_id", F.lit(None).cast("string"))
        fallback_candidates = band_fallbacks.where(F.col("fallback_band_id").isNotNull()).select(
            F.col("band_id").alias("source_band_id"),
            "fallback_band_id",
            "ordinal",
        )
        scoped = daily_clicks.drop("band_id").join(user_bands, "user_id", "left").join(
            fallback_candidates,
            user_bands.band_id == fallback_candidates.source_band_id,
            "inner",
        ).drop("band_id", "source_band_id").withColumnRenamed("fallback_band_id", "band_id").drop("ordinal")
        return global_facts.unionByName(scoped, allowMissingColumns=False)

    @step(input=[context_impressions, context_clicks, policy], output=query_signal_totals)
    def summarize_query(
        self, impression: DailyImpressions, click: DailyClicks, policy: RelevancePolicy
    ) -> QueryDocumentSignals:
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
        group_by(
            band_id=impression.band_id,
            query=impression.query,
            document_id=impression.document_id,
        )
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

    @step(input=[context_impressions, context_clicks, policy], output=popularity_totals)
    def summarize_popularity(
        self, impression: DailyImpressions, click: DailyClicks, policy: RelevancePolicy
    ) -> DocumentPopularity:
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
        group_by(band_id=impression.band_id, document_id=impression.document_id)
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
    def normalize_popularity(self, popularity: DocumentPopularity, policy: RelevancePolicy) -> DocumentPopularity:
        policy = cross_join(policy, allow_cartesian=True)
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
