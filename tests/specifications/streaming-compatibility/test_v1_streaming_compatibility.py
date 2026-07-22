import sys
from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.core.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.types import StructType
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def _compile(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False)


def _analysis(transform):
    return _compile(transform).analysis


def _recipe(transform) -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, _compile(transform).lowered)


def _body(transform) -> PySparkStepBody:
    return cast(PySparkStepBody, _analysis(transform).steps[0].plugin_body)


class StreamRaw(Schema):
    id = string(nullable=False)
    event_time = timestamp(nullable=False)


class StreamClean(Schema):
    id = string(nullable=False)


class StreamLookup(Schema):
    id = string(nullable=False)
    value = string(nullable=True)
    valid_from = timestamp(nullable=False)
    valid_to = timestamp(nullable=True)


class StreamEnriched(Schema):
    id = string(nullable=False)
    value = string(nullable=True)


class StreamOuter(Schema):
    id = string(nullable=True)
    value = string(nullable=True)


class StreamSummary(Schema):
    id = string(nullable=False)
    row_count = long(nullable=False)


class StreamWindowSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    id = string(nullable=False)
    row_count = long(nullable=False)


class StreamGlobalWindowSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    row_count = long(nullable=False)


@transform(streaming=True)
class StreamingSessionAggregate(Transform):
    rows = input(StreamRaw, streaming=True)
    summary = output(StreamWindowSummary)

    def summarize(self, row: StreamRaw) -> StreamWindowSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(bucket=session_window(row.event_time, "5 minutes"), id=row.id)
        return StreamWindowSummary(bucket=session_window(row.event_time, "5 minutes"), id=row.id, row_count=count())


@transform(streaming=True)
class StreamingGlobalSessionAggregate(Transform):
    rows = input(StreamRaw, streaming=True)
    summary = output(StreamGlobalWindowSummary)

    def summarize(self, row: StreamRaw) -> StreamGlobalWindowSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(bucket=session_window(row.event_time, "5 minutes"))
        return StreamGlobalWindowSummary(bucket=session_window(row.event_time, "5 minutes"), row_count=count())


def test_event_time_between_rejects_non_timestamp_expressions() -> None:
    with pytest.raises(TypeError, match="requires Timestamp Structure expressions"):
        event_time_between(lower("left"), lower("right"), upper="1 hour")


def test_watermark_rejects_non_timestamp_fields() -> None:
    @transform
    class InvalidWatermark(Transform):
        rows = input(StreamRaw, streaming=True)
        clean = output(StreamClean)

        def normalize(self, row: StreamRaw) -> StreamClean:
            watermark(row.id)
            return StreamClean(id=row.id)

    with pytest.raises(StructureCompileError, match="requires a Timestamp Structure field expression"):
        _compile(InvalidWatermark)


@pytest.mark.parametrize("delay", ["", "soon", "-1 second", "1 minute; SELECT 1"])
def test_watermark_rejects_invalid_delay_text(delay: str) -> None:
    @transform
    class InvalidWatermark(Transform):
        rows = input(StreamRaw, streaming=True)
        clean = output(StreamClean)

        def normalize(self, row: StreamRaw) -> StreamClean:
            watermark(row.event_time, delay=delay)
            return StreamClean(id=row.id)

    with pytest.raises(StructureCompileError, match="requires a non-negative fixed Spark interval"):
        _compile(InvalidWatermark)


@transform(streaming=True)
class StreamingProjection(Transform):
    rows = input(StreamRaw)
    clean = output(StreamClean)

    def normalize(self, row: StreamRaw) -> StreamClean:
        where(row.id.is_not_null())  # type: ignore[attr-defined]
        return StreamClean(id=row.id)


@transform(streaming=True)
class StreamingUnknownHook(Transform):
    rows = input(StreamRaw)
    clean = output(StreamClean)

    def normalize(self, row: StreamRaw) -> StreamClean:
        return StreamClean(id=row.id)

    @raw
    def arbitrary_hook(self, *, rows, spark, ctx):
        return rows


@transform(streaming=True)
class StreamingExists(Transform):
    rows = input(StreamRaw)
    lookups = input(StreamLookup)
    clean = output(StreamClean)

    def keep_known(self, row: StreamRaw, lookup: StreamLookup) -> StreamClean:
        where(exists(on=lookup.id == row.id))
        where(not_exists(on=lookup.value == row.id))
        return StreamClean(id=row.id)


@transform(streaming=True)
class StreamingJoinMany(Transform):
    rows = input(StreamRaw)
    lookups = input(StreamLookup)
    enriched = output(StreamEnriched)

    def expand(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        inner_join(on=lookup.id == row.id)
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingDedupedLookup(Transform):
    rows = input(StreamRaw)
    lookups = input(StreamLookup)
    enriched = output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        lookup_join(
            lookup,
            on=lookup.id == row.id,
            how=Join.LEFT,
            dedupe=JoinDedupe.latest_by(lookup.valid_from),
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingTemporalLookup(Transform):
    rows = input(StreamRaw)
    lookups = input(StreamLookup)
    enriched = output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        temporal_one(
            on=lookup.id == row.id,
            at=row.id,
            valid_from=lookup.valid_from,
            valid_to=lookup.valid_to,
            how=Join.LEFT,
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingAsOfLookup(Transform):
    rows = input(StreamRaw)
    lookups = input(StreamLookup)
    enriched = output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        as_of_one(
            on=lookup.id == row.id,
            left_time=row.id,
            right_time=lookup.valid_from,
            direction=AsOf.BACKWARD,
            how=Join.LEFT,
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingAggregate(Transform):
    rows = input(StreamRaw)
    summary = output(StreamSummary)

    def summarize(self, row: StreamRaw) -> StreamSummary:
        group_by(row.id)
        return StreamSummary(id=row.id, row_count=count())


@transform(streaming=True)
class StreamingWatermarkedAggregate(Transform):
    rows = input(StreamRaw, streaming=True)
    summary = output(StreamWindowSummary)

    def summarize(self, row: StreamRaw) -> StreamWindowSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(bucket=window(row.event_time, "10 minutes"), id=row.id)
        return StreamWindowSummary(bucket=window(row.event_time, "10 minutes"), id=row.id, row_count=count())


@transform(streaming=True)
class StreamingWatermarkedBusinessKeyAggregate(Transform):
    rows = input(StreamRaw, streaming=True)
    summary = output(StreamSummary)

    def summarize(self, row: StreamRaw) -> StreamSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(row.id)
        return StreamSummary(id=row.id, row_count=count())


@transform(streaming=True)
class StreamingSlidingAggregate(Transform):
    rows = input(StreamRaw, streaming=True)
    summary = output(StreamWindowSummary)

    def summarize(self, row: StreamRaw) -> StreamWindowSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(bucket=window(row.event_time, "10 minutes", "5 minutes"), id=row.id)
        return StreamWindowSummary(
            bucket=window(row.event_time, "10 minutes", "5 minutes"),
            id=row.id,
            row_count=count(),
        )


@transform(streaming=True)
class StreamingScalarUdf(Transform):
    rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    @special(type="udf", return_type=types.string(), nullable=False)
    def normalize(value: Any):
        return value.strip()

    def normalize_rows(self, row: StreamRaw) -> StreamClean:
        return StreamClean(id=self.normalize(row.id))


@transform(streaming=True)
class StreamingWatermarkedDedupe(Transform):
    rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def unique_rows(self, row: StreamRaw) -> StreamClean:
        watermark(row.event_time, delay="10 minutes")
        drop_duplicates(row.id)
        return StreamClean(id=row.id)


@transform(streaming=True)
class StreamingExplicitWatermarkedDedupe(Transform):
    rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def unique_rows(self, row: StreamRaw) -> StreamClean:
        watermark(row.event_time, delay="10 minutes")
        drop_duplicates_within_watermark(row.id)
        return StreamClean(id=row.id)


@transform(streaming=True)
class StreamingInnerStreamJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup, streaming=True)
    enriched = output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        watermark(row.event_time, delay="10 minutes")
        watermark(lookup.valid_from, delay="20 minutes")
        inner_join(
            lookup,
            on=(cast(Any, lookup).id == cast(Any, row).id)
            & event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingLeftOuterStreamJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup, streaming=True)
    enriched = output(StreamOuter)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamOuter:
        watermark(row.event_time, delay="10 minutes")
        watermark(lookup.valid_from, delay="20 minutes")
        rowset_join(
            lookup,
            how=Join.LEFT,
            on=(cast(Any, lookup).id == cast(Any, row).id)
            & event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
        )
        return StreamOuter(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingRightOuterStreamJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup, streaming=True)
    enriched = output(StreamOuter)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamOuter:
        watermark(row.event_time, delay="10 minutes")
        watermark(lookup.valid_from, delay="20 minutes")
        rowset_join(
            lookup,
            how=Join.RIGHT,
            on=(cast(Any, lookup).id == cast(Any, row).id)
            & event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
        )
        return StreamOuter(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingFullOuterStreamJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup, streaming=True)
    enriched = output(StreamOuter)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamOuter:
        watermark(row.event_time, delay="10 minutes")
        watermark(lookup.valid_from, delay="20 minutes")
        rowset_join(
            lookup,
            how=Join.FULL,
            on=(cast(Any, lookup).id == cast(Any, row).id)
            & event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
        )
        return StreamOuter(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingSemiStreamJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup, streaming=True)
    clean = output(StreamClean)

    def keep_known(self, row: StreamRaw, lookup: StreamLookup) -> StreamClean:
        watermark(row.event_time, delay="10 minutes")
        watermark(lookup.valid_from, delay="20 minutes")
        where(
            exists(
                lookup,
                on=(cast(Any, lookup).id == cast(Any, row).id)
                & event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
            )
        )
        return StreamClean(id=row.id)


@transform(streaming=True)
class StreamingStaticAntiJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup)
    clean = output(StreamClean)

    def discard_known(self, row: StreamRaw, lookup: StreamLookup) -> StreamClean:
        where(not_exists(lookup, on=lookup.id == row.id))
        return StreamClean(id=row.id)


@transform(streaming=True)
class StreamingUnmarkedSideJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup)
    enriched = output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        watermark(row.event_time, delay="10 minutes")
        watermark(lookup.valid_from, delay="20 minutes")
        inner_join(
            lookup,
            on=(cast(Any, lookup).id == cast(Any, row).id)
            & event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingOneSidedStreamJoin(Transform):
    rows = input(StreamRaw)
    lookups = input(StreamLookup, streaming=True)
    enriched = output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        watermark(row.event_time, delay="10 minutes")
        watermark(lookup.valid_from, delay="20 minutes")
        inner_join(
            lookup,
            on=(cast(Any, lookup).id == cast(Any, row).id)
            & event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
        )
        return StreamEnriched(id=row.id, value=lookup.value)


def test_v1_streaming_projection_filter_and_schema_validation_are_compatible_without_spark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    plan = _analysis(StreamingProjection)
    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert report.support is StreamingSupport.COMPATIBLE
    assert report.required
    assert report.findings == ()


def test_v2_stream_static_analytical_joins_are_compatible_without_spark() -> None:
    for transform_type in (StreamingExists, StreamingJoinMany):
        plan = _analysis(transform_type)
        report = Compiler.compileability.streaming()(
            PySpark.compiler.lower()(plan),
            required=bool((plan.options or {})["streaming"]),
        )

        assert report.support is StreamingSupport.COMPATIBLE
        assert report.findings == ()


def test_v2_windowed_lookup_joins_are_batch_only_without_spark() -> None:
    expected = {
        StreamingDedupedLookup: "deduped lookup join lookup",
        StreamingTemporalLookup: "temporal join lookup",
        StreamingAsOfLookup: "as-of join lookup",
    }

    for transform_type, operation in expected.items():
        plan = _analysis(transform_type)
        report = Compiler.compileability.streaming()(
            PySpark.compiler.lower()(plan),
            required=bool((plan.options or {})["streaming"]),
        )

        assert report.support is StreamingSupport.BATCH_ONLY
        assert len(report.findings) == 1
        assert report.findings[0].code == "STREAM-E0801"
        assert report.findings[0].operation == operation


def test_v2_grouped_aggregates_are_batch_only_without_spark() -> None:
    plan = _analysis(StreamingAggregate)

    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.BATCH_ONLY
    assert len(report.findings) == 1
    assert report.findings[0].code == "STREAM-E0801"
    assert report.findings[0].operation == "grouped aggregate"
    assert "watermark" in report.findings[0].problem


def test_v4_watermarked_business_key_aggregate_reports_unbounded_state() -> None:
    unbounded = _analysis(StreamingWatermarkedBusinessKeyAggregate)
    report = Compiler.compileability.streaming()(PySpark.compiler.lower()(unbounded), required=True)

    assert report.support is StreamingSupport.BATCH_ONLY
    assert report.findings[0].operation == "unbounded grouped aggregate"


def test_v4_event_time_window_has_time_window_schema_and_rejects_mixed_signature() -> None:
    aggregate = next(operation.aggregate for operation in _body(StreamingWatermarkedAggregate).operations if operation.aggregate is not None)

    assert aggregate is not None
    assert isinstance(aggregate.keys[0].expression.type, StructType)
    assert aggregate.keys[0].expression.type.schema is TimeWindow
    with pytest.raises(TypeError, match="cannot mix event-time arguments"):
        window("event_time", "10 minutes", partition_by="id")  # type: ignore[call-overload]


def test_v4_session_window_aggregate_requires_a_business_key_and_reports_append_only() -> None:
    report = Compiler.compileability.streaming()(_recipe(StreamingSessionAggregate), required=True)
    operation = next(operation for operation in _body(StreamingSessionAggregate).operations if operation.aggregate is not None)

    assert report.support is StreamingSupport.COMPATIBLE
    assert operation.streaming_output_modes == (StreamingOutputMode.APPEND,)

    global_session = _analysis(StreamingGlobalSessionAggregate)
    report = Compiler.compileability.streaming()(PySpark.compiler.lower()(global_session), required=True)

    assert report.support is StreamingSupport.BATCH_ONLY
    assert report.findings[0].operation == "session-window aggregate"
    assert "business grouping key" in report.findings[0].problem


def test_v4_sliding_window_renders_with_positional_slide() -> None:
    plan = _recipe(StreamingSlidingAggregate)
    rendered = "\n".join(
        PySpark.render.project()(
            plan,
            source_transform="tests.fixtures.streaming.transforms.StreamingSlidingAggregate",
            generated_package="streaming_generated",
            source_schema_modules={"tests.fixtures.streaming.schemas": [StreamRaw, StreamWindowSummary]},
        ).values()
    )

    assert "F.window(F.col(\"stream_raw.event_time\"), '10 minutes', '5 minutes')" in rendered


def test_v4_scalar_udf_is_a_compatible_row_local_streaming_expression() -> None:
    plan = _analysis(StreamingScalarUdf)
    report = Compiler.compileability.streaming()(PySpark.compiler.lower()(plan), required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v2_windowed_watermarked_aggregate_is_streaming_without_spark() -> None:
    plan = _analysis(StreamingWatermarkedAggregate)

    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v2_watermarked_dedupe_is_streaming_without_spark() -> None:
    plan = _analysis(StreamingWatermarkedDedupe)

    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v4_explicit_watermarked_dedupe_is_streaming_without_spark() -> None:
    plan = _analysis(StreamingExplicitWatermarkedDedupe)

    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v2_inner_stream_stream_join_is_compatible_with_watermarks_and_time_bounds() -> None:
    plan = _analysis(StreamingInnerStreamJoin)

    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


@pytest.mark.parametrize(
    "transform_type",
    [
        StreamingLeftOuterStreamJoin,
        StreamingRightOuterStreamJoin,
        StreamingFullOuterStreamJoin,
        StreamingSemiStreamJoin,
    ],
)
def test_v4_bounded_outer_and_semi_stream_stream_joins_are_compatible(transform_type: type[Transform]) -> None:
    plan = _analysis(transform_type)
    report = Compiler.compileability.streaming()(PySpark.compiler.lower()(plan), required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v4_stream_static_anti_join_is_batch_only() -> None:
    plan = _analysis(StreamingStaticAntiJoin)
    report = Compiler.compileability.streaming()(PySpark.compiler.lower()(plan), required=True)

    assert report.support is StreamingSupport.BATCH_ONLY
    assert report.findings[0].operation == "stream-static anti join lookup"


def test_v4_outer_and_semi_join_explain_output_names_append_requirement() -> None:
    from structure.core.cli.api import CliApp

    outer = CliApp.render_explain_report()(StreamingFullOuterStreamJoin)
    semi = CliApp.render_explain_report()(StreamingSemiStreamJoin)

    assert "lookup rowset_join row_multiplying streaming_modes=append" in outer
    assert "lookup exists row_filtering streaming_modes=append" in semi


def test_v2_unmarked_joined_input_keeps_stream_static_semantics() -> None:
    plan = _analysis(StreamingUnmarkedSideJoin)

    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v2_stream_stream_join_requires_both_inputs_declared_streaming() -> None:
    plan = _analysis(StreamingOneSidedStreamJoin)

    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.BATCH_ONLY
    assert len(report.findings) == 1
    assert report.findings[0].operation == "stream-stream join lookup"
    assert "streaming=True" in report.findings[0].problem
    assert "streaming=True" in report.findings[0].use


def test_v1_streaming_unsafe_hook_is_unknown_with_registered_finding() -> None:
    plan = _analysis(StreamingUnknownHook)

    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.UNKNOWN
    assert len(report.findings) == 1
    finding = report.findings[0]
    assert finding.code == "STREAM-W0801"
    assert finding.step == "normalize"
    assert finding.operation == "raw hook arbitrary_hook"
    assert finding.to_diagnostic().docs == "docs/Diagnostics.md#stream-w0801"


def test_v1_streaming_report_is_included_in_explain_output() -> None:
    from structure.core.cli.api import CliApp

    report = CliApp.render_explain_report()(StreamingUnknownHook)

    assert "streaming:" in report
    assert "status: unknown" in report
    assert "required: true" in report
    assert "STREAM-W0801: unknown in normalize (raw hook arbitrary_hook)" in report


def test_v2_aggregate_streaming_report_is_included_in_explain_output() -> None:
    from structure.core.cli.api import CliApp

    report = CliApp.render_explain_report()(StreamingAggregate)

    assert "status: batch_only" in report
    assert "STREAM-E0801: batch_only in summarize (grouped aggregate)" in report
    assert "operations: aggregate(aggregate keys=id metrics=count streaming_modes=append|update)" in report


def test_v2_watermarked_aggregate_explain_output_names_policy() -> None:
    from structure.core.cli.api import CliApp

    report = CliApp.render_explain_report()(StreamingWatermarkedAggregate)

    assert "status: compatible" in report
    assert (
        "operations: watermark(event_time 10 minutes), aggregate(aggregate keys=bucket,id metrics=count streaming_modes=append|update)"
        in report
    )


def test_v4_session_aggregate_explain_output_names_append_only_policy() -> None:
    from structure.core.cli.api import CliApp

    report = CliApp.render_explain_report()(StreamingSessionAggregate)

    assert "status: compatible" in report
    assert "aggregate(aggregate keys=bucket,id metrics=count streaming_modes=append)" in report


def test_v2_analytical_join_explain_output_names_join_shapes() -> None:
    from structure.core.cli.api import CliApp

    exists_report = CliApp.render_explain_report()(StreamingExists)
    many_report = CliApp.render_explain_report()(StreamingJoinMany)
    dedupe_report = CliApp.render_explain_report()(StreamingDedupedLookup)
    temporal_report = CliApp.render_explain_report()(StreamingTemporalLookup)
    as_of_report = CliApp.render_explain_report()(StreamingAsOfLookup)

    assert "operations: exists(row_filtering), not_exists(row_filtering)" in exists_report
    assert "joins: lookup exists row_filtering, lookup not_exists row_filtering" in exists_report
    assert "operations: rowset_join(row_multiplying)" in many_report
    assert "joins: lookup rowset_join row_multiplying" in many_report
    assert "dedupe=latest/error" in dedupe_report
    assert "temporal=closed_open/error" in temporal_report
    assert "as_of=backward/error" in as_of_report


def test_v1_generated_streaming_code_avoids_streaming_lifecycle_and_actions() -> None:
    plan = _recipe(StreamingProjection)
    files = PySpark.render.project()(
        plan,
        source_transform="tests.fixtures.streaming.transforms.StreamingProjection",
        generated_package="streaming_generated",
        source_schema_modules={"tests.fixtures.streaming.schemas": [StreamRaw, StreamClean]},
    )

    generated = "\n".join(files.values())

    forbidden = ("readStream", "writeStream", "collect(", "count(", "toPandas", ".rdd")
    assert all(value not in generated for value in forbidden)


def test_v2_generated_watermark_code_avoids_streaming_lifecycle_and_actions() -> None:
    plan = _recipe(StreamingWatermarkedAggregate)
    files = PySpark.render.project()(
        plan,
        source_transform="tests.fixtures.streaming.transforms.StreamingWatermarkedAggregate",
        generated_package="streaming_generated",
        source_schema_modules={"tests.fixtures.streaming.schemas": [StreamRaw, StreamWindowSummary]},
    )

    generated = "\n".join(files.values())

    assert '.withWatermark("event_time", ' in generated
    assert "F.window(F.col(\"stream_raw.event_time\"), '10 minutes')" in generated
    forbidden = (
        "readStream",
        "writeStream",
        "start(",
        "awaitTermination",
        "checkpoint",
        "collect(",
        "toPandas",
        ".rdd",
    )
    assert all(value not in generated for value in forbidden)


def test_v4_generated_dedupe_uses_only_the_permitted_streaming_branch() -> None:
    files = PySpark.render.project()(
        _recipe(StreamingWatermarkedDedupe),
        source_transform="tests.fixtures.streaming.transforms.StreamingWatermarkedDedupe",
        generated_package="streaming_generated",
        source_schema_modules={"tests.fixtures.streaming.schemas": [StreamRaw, StreamClean]},
    )

    generated = "\n".join(files.values())

    assert ".isStreaming:" in generated
    assert ".dropDuplicatesWithinWatermark([\"id\"])" in generated
    assert ".dropDuplicates([\"id\"])" in generated
    assert all(
        value not in generated for value in ("readStream", "writeStream", "start(", "awaitTermination", "checkpoint")
    )


def test_v4_generated_explicit_dedupe_has_no_adaptive_branch() -> None:
    files = PySpark.render.project()(
        _recipe(StreamingExplicitWatermarkedDedupe),
        source_transform="tests.fixtures.streaming.transforms.StreamingExplicitWatermarkedDedupe",
        generated_package="streaming_generated",
        source_schema_modules={"tests.fixtures.streaming.schemas": [StreamRaw, StreamClean]},
    )

    generated = "\n".join(files.values())

    assert ".dropDuplicatesWithinWatermark([\"id\"])" in generated
    assert ".isStreaming" not in generated
