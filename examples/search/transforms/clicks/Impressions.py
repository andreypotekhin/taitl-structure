"""Streaming daily impression facts."""

from examples.search.schemas.clicks import DailyImpressions, Impression
from structure import StreamingMode, Transform, input, output, transform
from structure.plugin.pyspark import (
    count,
    drop_duplicates_within_watermark,
    group_by,
    lower,
    regexp_replace,
    trim,
    watermark,
    window,
)


@transform(streaming_compatible=True)
class Impressions(Transform):
    """Summarize deduplicated document impressions into daily facts."""

    impressions = input(Impression, streaming=StreamingMode.YES)
    daily_impressions = output(DailyImpressions)

    def summarize(self, impression: Impression) -> DailyImpressions:
        watermark(impression.shown_at, delay="7 days")
        drop_duplicates_within_watermark(impression.id)
        day = window(impression.shown_at, "1 day")
        query = lower(regexp_replace(trim(impression.query), pattern=r"\s+", replacement=" "))
        group_by(
            window=day,
            query=query,
            document_id=impression.document_id,
            position=impression.position,
            examination_propensity=impression.examination_propensity,
        )
        return DailyImpressions(
            window=day,
            query=query,
            document_id=impression.document_id,
            position=impression.position,
            examination_propensity=impression.examination_propensity,
            impression_count=count(),
        )
