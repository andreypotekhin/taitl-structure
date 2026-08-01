from examples.store.schemas.merchandising import (
    DailyRecommendationClicks,
    DailyRecommendationImpressions,
    ProductRecommendationSignal,
    ProductRecommendationSignalTotals,
    RecommendationClick,
    RecommendationImpression,
)
from structure import *
from structure.plugin.pyspark import *


@transform(streaming=True)
class BuildProductSignals(Transform):
    impressions = input(RecommendationImpression, streaming=True)
    clicks = input(RecommendationClick, streaming=True)
    impression_facts = lane(DailyRecommendationImpressions)
    click_facts = lane(DailyRecommendationClicks)
    signal_totals = lane(ProductRecommendationSignalTotals)
    daily_impressions = output(DailyRecommendationImpressions)
    daily_clicks = output(DailyRecommendationClicks)
    signals = output(ProductRecommendationSignal)

    @step(input=impressions, output=impression_facts)
    def summarize_impressions(self, impression: RecommendationImpression) -> DailyRecommendationImpressions:
        watermark(impression.shown_at, delay="7 days")
        drop_duplicates_within_watermark(impression.id)
        day = window(impression.shown_at, "1 day")
        group_by(
            window=day,
            tenant_id=impression.tenant.tenant_id,
            strategy_id=impression.strategy_id,
            policy_version=impression.policy_version,
            product_id=impression.product_id,
            rank=impression.rank,
            examination_propensity=impression.examination_propensity,
        )
        return DailyRecommendationImpressions.project(impression)(
            window=day,
            impression_count=count(),
        )

    @step(input=[impressions, clicks], output=click_facts)
    def summarize_clicks(
        self, impression: RecommendationImpression, click: RecommendationClick
    ) -> DailyRecommendationClicks:
        watermark(impression.shown_at, delay="7 days")
        watermark(click.occurred_at, delay="7 days")
        drop_duplicates_within_watermark(impression.id)
        drop_duplicates_within_watermark(click.id)
        inner_join(
            click,
            on=(click.impression_id == impression.id)
            & event_time_between(impression.shown_at, click.occurred_at, upper="24 hours"),
        )
        day = window(impression.shown_at, "1 day")
        group_by(
            window=day,
            tenant_id=impression.tenant.tenant_id,
            strategy_id=impression.strategy_id,
            policy_version=impression.policy_version,
            product_id=impression.product_id,
            rank=impression.rank,
            examination_propensity=impression.examination_propensity,
        )
        return DailyRecommendationClicks.project(impression)(
            window=day,
            click_count=count(),
            clicked_impression_count=count_distinct(click.impression_id),
        )

    @step(input=impression_facts, output=daily_impressions)
    def publish_daily_impressions(self, impression: DailyRecommendationImpressions) -> DailyRecommendationImpressions:
        return DailyRecommendationImpressions.project(impression)

    @step(input=click_facts, output=daily_clicks)
    def publish_daily_clicks(self, click: DailyRecommendationClicks) -> DailyRecommendationClicks:
        return DailyRecommendationClicks.project(click)

    @step(input=[impression_facts, click_facts], output=signal_totals)
    def summarize_signals(
        self, impression: DailyRecommendationImpressions, click: DailyRecommendationClicks
    ) -> ProductRecommendationSignalTotals:
        left_join(
            click,
            on=(click.window == impression.window)
            & (click.tenant.tenant_id == impression.tenant.tenant_id)
            & (click.strategy_id == impression.strategy_id)
            & (click.policy_version == impression.policy_version)
            & (click.product_id == impression.product_id)
            & (click.rank == impression.rank)
            & (click.examination_propensity == impression.examination_propensity),
        )
        clicks = coalesce(click.click_count, 0)
        clicked = coalesce(click.clicked_impression_count, 0)
        propensity = when(impression.examination_propensity > 0.0, impression.examination_propensity).otherwise(1.0)
        group_by(
            tenant_id=impression.tenant.tenant_id,
            strategy_id=impression.strategy_id,
            product_id=impression.product_id,
        )
        impression_count = sum(impression.impression_count)
        click_count = sum(clicks)
        clicked_impression_count = sum(clicked)
        exposure_weight = sum(impression.impression_count / propensity)
        click_weight = sum(clicked / propensity)
        return ProductRecommendationSignalTotals.project(impression)(
            impression_count=impression_count,
            clicked_impression_count=clicked_impression_count,
            raw_click_count=click_count,
            click_through_rate=sum(0.0),
            exposure_adjusted_click_rate=sum(0.0),
            exposure_weight=exposure_weight,
            click_weight=click_weight,
            attributed_purchase_count=sum(0),
            conversion_rate=sum(0.0),
        )

    @step(input=signal_totals, output=signals)
    def publish_signals(self, signal: ProductRecommendationSignalTotals) -> ProductRecommendationSignal:
        return ProductRecommendationSignal.project(signal)(
            click_through_rate=when(
                signal.impression_count > 0,
                signal.clicked_impression_count / signal.impression_count,
            ).otherwise(None),
            exposure_adjusted_click_rate=when(
                signal.exposure_weight > 0.0,
                signal.click_weight / signal.exposure_weight,
            ).otherwise(None),
            attributed_purchase_count=signal.attributed_purchase_count,
            conversion_rate=when(
                signal.impression_count > 0,
                signal.attributed_purchase_count / signal.impression_count,
            ).otherwise(None),
        )
