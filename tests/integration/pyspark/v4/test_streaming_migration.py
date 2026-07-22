from typing import Any, cast

import pytest
from integration.pyspark.support.backend_matrix import session

from structure import *
from structure.plugin.pyspark import *

pytestmark = pytest.mark.integration


class Event(Schema):
    id = string(nullable=False)
    event_time = timestamp(nullable=False)


class SessionSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    id = string(nullable=False)
    rows = long(nullable=False)


class OuterEvent(Schema):
    id = string(nullable=True)


@transform(streaming=True)
class Sessionize(Transform):
    events = input(Event, streaming=True)
    summaries = output(SessionSummary)

    def summarize(self, event: Event) -> SessionSummary:
        watermark(event.event_time, delay="1 minute")
        group_by(bucket=session_window(event.event_time, "30 seconds"), id=event.id)
        return SessionSummary(bucket=session_window(event.event_time, "30 seconds"), id=event.id, rows=count())


@transform(streaming=True)
class Correlate(Transform):
    left = input(Event, streaming=True)
    right = input(Event, streaming=True)
    correlated = output(OuterEvent)

    def correlate(self, left: Event, right: Event) -> OuterEvent:
        watermark(left.event_time, delay="1 minute")
        watermark(right.event_time, delay="1 minute")
        rowset_join(
            right,
            how=Join.FULL,
            on=(cast(Any, left).id == cast(Any, right).id)
            & event_time_between(left.event_time, right.event_time, upper="30 seconds"),
        )
        return OuterEvent(id=left.id)


def test_v4_caller_owned_streaming_transforms_return_streaming_dataframes(spark) -> None:
    source = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 1)
        .load()
        .selectExpr("CAST(value AS STRING) AS id", "timestamp AS event_time")
    )

    sessionized = Sessionize(events=source).run(session(spark, execution_mode="online")).summaries
    correlated = Correlate(left=source, right=source).run(session(spark, execution_mode="online")).correlated

    assert sessionized.isStreaming
    assert correlated.isStreaming
