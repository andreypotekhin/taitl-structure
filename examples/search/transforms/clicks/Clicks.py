"""Streaming attributed click facts."""

from examples.search.schemas.clicks import Click, DailyClicks, Impression
from structure import Transform, input, output, transform
from structure.plugin.pyspark import (
    count,
    count_distinct,
    drop_duplicates_within_watermark,
    event_time_between,
    group_by,
    inner_join,
    lower,
    regexp_replace,
    sum,
    trim,
    watermark,
    when,
    window,
)


@transform(streaming=True)
class Clicks(Transform):
    """Attribute deduplicated clicks to their impressions before aggregation."""

    impressions = input(Impression, streaming=True)
    clicks = input(Click, streaming=True)
    daily_clicks = output(DailyClicks)

    def summarize(self, impression: Impression, click: Click) -> DailyClicks:
        """Attribute clicks to their displayed user's daily exposure grain."""

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
        query = lower(regexp_replace(trim(impression.query), pattern=r"\s+", replacement=" "))
        dwell = when(click.dwell_seconds > 0.0, click.dwell_seconds).otherwise(0.0)
        credit = when(dwell < 60.0, dwell).otherwise(60.0) / 60.0
        group_by(
            window=day,
            query=query,
            document_id=impression.document_id,
            position=impression.position,
            examination_propensity=impression.examination_propensity,
            user_id=impression.user_id,
            band_id=None,
        )
        return DailyClicks.project(impression)(
            window=day,
            query=query,
            user_id=impression.user_id,
            band_id=None,
            click_count=count(),
            clicked_impression_count=count_distinct(click.impression_id),
            dwell_seconds=sum(dwell),
            dwell_credit=sum(credit),
            long_click_count=sum(when(dwell >= 10.0, 1).otherwise(0)),
        )
