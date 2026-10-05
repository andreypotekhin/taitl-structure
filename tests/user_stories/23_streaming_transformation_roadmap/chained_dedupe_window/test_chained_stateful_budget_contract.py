from __future__ import annotations

from typing import cast

from structure import *
from structure.core.compiler.api import Compiler
from structure.core.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class RawEvent(Schema):
    event_id = string(nullable=False)
    event_time = timestamp(nullable=False)


class WindowSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    row_count = long(nullable=False)


@transform(streaming=True)
class EventSummary(Transform):
    events = input(RawEvent, streaming=True)
    summary = output(WindowSummary)
    memory_budget = budget(memory_source="prefer_spark", fallback_mb=768)

    def summarize(self, event: RawEvent) -> WindowSummary:
        watermark(event.event_time, delay="10 minutes")
        drop_duplicates_within_watermark(event.event_id)
        budget(max_rows=500_000, max_state_bytes=268_435_456)
        group_by(bucket=window(event.event_time, "5 minutes"))
        budget(max_rows=2_000_000, max_state_bytes=1_073_741_824)
        return WindowSummary(bucket=window(event.event_time, "5 minutes"), row_count=count())


def test_developer_can_declare_and_inspect_budgets_for_the_admitted_stateful_pair() -> None:
    compilation = Compiler.frontend.compile()(EventSummary, materialize_schemas=False)
    plan = cast(PySparkExecutionPlan, compilation.lowered)
    report = Compiler.compileability.streaming()(plan, required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()
    assert [stage.output_modes for stage in report.stages] == [("append",), ("append",)]
    assert [(stage.max_rows, stage.max_state_bytes) for stage in report.stages] == [
        (500_000, 268_435_456),
        (2_000_000, 1_073_741_824),
    ]
