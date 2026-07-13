import sys
from typing import Any, cast

import pytest

import structure
from structure.app.compiler.api import Compiler
from structure.app.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class StreamRaw(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    event_time = structure.field(structure.Timestamp(), nullable=False)


class StreamClean(structure.Schema):
    id = structure.field(structure.String(), nullable=False)


class StreamLookup(structure.Schema):
    id = structure.field(structure.String(), nullable=False, primary_key=True)
    value = structure.field(structure.String(), nullable=True)
    valid_from = structure.field(structure.Timestamp(), nullable=False)
    valid_to = structure.field(structure.Timestamp(), nullable=True)


class StreamEnriched(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    value = structure.field(structure.String(), nullable=True)


class StreamSummary(structure.Schema):
    id = structure.field(structure.String(), nullable=False)
    row_count = structure.field(structure.Long(), nullable=False)


def test_event_time_between_rejects_non_timestamp_expressions() -> None:
    with pytest.raises(TypeError, match="requires Timestamp Structure expressions"):
        structure.event_time_between(structure.lower("left"), structure.lower("right"), upper="1 hour")


@structure.transform(streaming_compatible=True)
class StreamingProjection(structure.Transform):
    rows = structure.input(StreamRaw)
    clean = structure.output(StreamClean)

    def normalize(self, row: StreamRaw) -> StreamClean:
        structure.where(row.id.is_not_null())  # type: ignore[attr-defined]
        return StreamClean(id=row.id)


@structure.transform(streaming_compatible=True)
class StreamingUnknownHook(structure.Transform):
    rows = structure.input(StreamRaw)
    clean = structure.output(StreamClean)

    def normalize(self, row: StreamRaw) -> StreamClean:
        return StreamClean(id=row.id)

    @structure.raw
    def arbitrary_hook(self, *, rows, spark, ctx):
        return rows


@structure.transform(streaming_compatible=True)
class StreamingExists(structure.Transform):
    rows = structure.input(StreamRaw)
    lookups = structure.input(StreamLookup)
    clean = structure.output(StreamClean)

    def keep_known(self, row: StreamRaw, lookup: StreamLookup) -> StreamClean:
        structure.where(structure.exists(on=lookup.id == row.id))
        structure.where(structure.not_exists(on=lookup.value == row.id))
        return StreamClean(id=row.id)


@structure.transform(streaming_compatible=True)
class StreamingJoinMany(structure.Transform):
    rows = structure.input(StreamRaw)
    lookups = structure.input(StreamLookup)
    enriched = structure.output(StreamEnriched)

    def expand(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        structure.inner_join(on=lookup.id == row.id)
        return StreamEnriched(id=row.id, value=lookup.value)


@structure.transform(streaming_compatible=True)
class StreamingDedupedLookup(structure.Transform):
    rows = structure.input(StreamRaw)
    lookups = structure.input(StreamLookup)
    enriched = structure.output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        structure.lookup_join(
            lookup,
            on=lookup.id == row.id,
            how=structure.Join.LEFT,
            dedupe=structure.JoinDedupe.latest_by(lookup.valid_from),
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@structure.transform(streaming_compatible=True)
class StreamingTemporalLookup(structure.Transform):
    rows = structure.input(StreamRaw)
    lookups = structure.input(StreamLookup)
    enriched = structure.output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        structure.temporal_one(
            on=lookup.id == row.id,
            at=row.id,
            valid_from=lookup.valid_from,
            valid_to=lookup.valid_to,
            how=structure.Join.LEFT,
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@structure.transform(streaming_compatible=True)
class StreamingAsOfLookup(structure.Transform):
    rows = structure.input(StreamRaw)
    lookups = structure.input(StreamLookup)
    enriched = structure.output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        structure.as_of_one(
            on=lookup.id == row.id,
            left_time=row.id,
            right_time=lookup.valid_from,
            direction=structure.AsOf.BACKWARD,
            how=structure.Join.LEFT,
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@structure.transform(streaming_compatible=True)
class StreamingAggregate(structure.Transform):
    rows = structure.input(StreamRaw)
    summary = structure.output(StreamSummary)

    def summarize(self, row: StreamRaw) -> StreamSummary:
        structure.group_by(row.id)
        return StreamSummary(id=row.id, row_count=structure.count())


@structure.transform(streaming_compatible=True)
class StreamingWatermarkedAggregate(structure.Transform):
    rows = structure.input(StreamRaw, streaming=structure.StreamingMode.YES)
    summary = structure.output(StreamSummary)

    def summarize(self, row: StreamRaw) -> StreamSummary:
        structure.watermark(row.event_time, delay="10 minutes")
        structure.group_by(row.id)
        return StreamSummary(id=row.id, row_count=structure.count())


@structure.transform(streaming_compatible=True)
class StreamingWatermarkedDedupe(structure.Transform):
    rows = structure.input(StreamRaw, streaming=structure.StreamingMode.YES)
    clean = structure.output(StreamClean)

    def unique_rows(self, row: StreamRaw) -> StreamClean:
        structure.watermark(row.event_time, delay="10 minutes")
        structure.drop_duplicates(row.id)
        return StreamClean(id=row.id)


@structure.transform(streaming_compatible=True)
class StreamingInnerStreamJoin(structure.Transform):
    rows = structure.input(StreamRaw, streaming=structure.StreamingMode.YES)
    lookups = structure.input(StreamLookup, streaming=structure.StreamingMode.YES)
    enriched = structure.output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        structure.watermark(row.event_time, delay="10 minutes")
        structure.watermark(lookup.valid_from, delay="20 minutes")
        structure.inner_join(
            lookup,
            on=(cast(Any, lookup).id == cast(Any, row).id)
            & structure.event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@structure.transform(streaming_compatible=True)
class StreamingUnmarkedSideJoin(structure.Transform):
    rows = structure.input(StreamRaw, streaming=structure.StreamingMode.YES)
    lookups = structure.input(StreamLookup)
    enriched = structure.output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        structure.watermark(row.event_time, delay="10 minutes")
        structure.watermark(lookup.valid_from, delay="20 minutes")
        structure.inner_join(
            lookup,
            on=(cast(Any, lookup).id == cast(Any, row).id)
            & structure.event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
        )
        return StreamEnriched(id=row.id, value=lookup.value)


@structure.transform(streaming_compatible=True)
class StreamingOneSidedStreamJoin(structure.Transform):
    rows = structure.input(StreamRaw)
    lookups = structure.input(StreamLookup, streaming=structure.StreamingMode.YES)
    enriched = structure.output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        structure.watermark(row.event_time, delay="10 minutes")
        structure.watermark(lookup.valid_from, delay="20 minutes")
        structure.inner_join(
            lookup,
            on=(cast(Any, lookup).id == cast(Any, row).id)
            & structure.event_time_between(cast(Any, row).event_time, cast(Any, lookup).valid_from, upper="1 hour"),
        )
        return StreamEnriched(id=row.id, value=lookup.value)


def test_v1_streaming_projection_filter_and_schema_validation_are_compatible_without_spark() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    plan = compile_transform(StreamingProjection)
    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert report.support is StreamingSupport.COMPATIBLE
    assert report.required
    assert report.findings == ()


def test_v2_stream_static_analytical_joins_are_compatible_without_spark() -> None:
    for transform_type in (StreamingExists, StreamingJoinMany):
        plan = compile_transform(transform_type)
        report = Compiler.compileability.streaming()(
            PySpark.plan.lower()(plan),
            required=bool((plan.options or {})["streaming_compatible"]),
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
        plan = compile_transform(transform_type)
        report = Compiler.compileability.streaming()(
            PySpark.plan.lower()(plan),
            required=bool((plan.options or {})["streaming_compatible"]),
        )

        assert report.support is StreamingSupport.BATCH_ONLY
        assert len(report.findings) == 1
        assert report.findings[0].code == "STREAM-E0801"
        assert report.findings[0].operation == operation


def test_v2_grouped_aggregates_are_batch_only_without_spark() -> None:
    plan = compile_transform(StreamingAggregate)

    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.BATCH_ONLY
    assert len(report.findings) == 1
    assert report.findings[0].code == "STREAM-E0801"
    assert report.findings[0].operation == "grouped aggregate"
    assert "watermark" in report.findings[0].problem


def test_v2_watermarked_aggregate_is_streaming_compatible_without_spark() -> None:
    plan = compile_transform(StreamingWatermarkedAggregate)

    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v2_watermarked_dedupe_is_streaming_compatible_without_spark() -> None:
    plan = compile_transform(StreamingWatermarkedDedupe)

    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v2_inner_stream_stream_join_is_compatible_with_watermarks_and_time_bounds() -> None:
    plan = compile_transform(StreamingInnerStreamJoin)

    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v2_unmarked_joined_input_keeps_stream_static_semantics() -> None:
    plan = compile_transform(StreamingUnmarkedSideJoin)

    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v2_stream_stream_join_requires_both_inputs_declared_streaming() -> None:
    plan = compile_transform(StreamingOneSidedStreamJoin)

    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.BATCH_ONLY
    assert len(report.findings) == 1
    assert report.findings[0].operation == "stream-stream join lookup"
    assert "StreamingMode.YES" in report.findings[0].problem
    assert "StreamingMode.YES" in report.findings[0].use


def test_v1_streaming_unsafe_hook_is_unknown_with_registered_finding() -> None:
    plan = compile_transform(StreamingUnknownHook)

    report = Compiler.compileability.streaming()(
        PySpark.plan.lower()(plan),
        required=bool((plan.options or {})["streaming_compatible"]),
    )

    assert report.support is StreamingSupport.UNKNOWN
    assert len(report.findings) == 1
    finding = report.findings[0]
    assert finding.code == "STREAM-W0801"
    assert finding.step == "normalize"
    assert finding.operation == "raw hook arbitrary_hook"
    assert finding.to_diagnostic().docs == "docs/Diagnostics.md#stream-w0801"


def test_v1_streaming_report_is_included_in_explain_output() -> None:
    from structure.app.cli.api import CliApp

    report = CliApp.render_explain_report()(StreamingUnknownHook)

    assert "streaming:" in report
    assert "status: unknown" in report
    assert "required: true" in report
    assert "STREAM-W0801: unknown in normalize (raw hook arbitrary_hook)" in report


def test_v2_aggregate_streaming_report_is_included_in_explain_output() -> None:
    from structure.app.cli.api import CliApp

    report = CliApp.render_explain_report()(StreamingAggregate)

    assert "status: batch_only" in report
    assert "STREAM-E0801: batch_only in summarize (grouped aggregate)" in report
    assert "operations: aggregate(aggregate keys=id metrics=count streaming_modes=update|complete)" in report


def test_v2_watermarked_aggregate_explain_output_names_policy() -> None:
    from structure.app.cli.api import CliApp

    report = CliApp.render_explain_report()(StreamingWatermarkedAggregate)

    assert "status: compatible" in report
    assert (
        "operations: watermark(event_time 10 minutes), aggregate(aggregate keys=id metrics=count streaming_modes=update|complete)"
        in report
    )


def test_v2_analytical_join_explain_output_names_join_shapes() -> None:
    from structure.app.cli.api import CliApp

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


def test_v1_generated_streaming_compatible_code_avoids_streaming_lifecycle_and_actions() -> None:
    plan = PySpark.plan.lower()(compile_transform(StreamingProjection))
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
    plan = PySpark.plan.lower()(compile_transform(StreamingWatermarkedAggregate))
    files = PySpark.render.project()(
        plan,
        source_transform="tests.fixtures.streaming.transforms.StreamingWatermarkedAggregate",
        generated_package="streaming_generated",
        source_schema_modules={"tests.fixtures.streaming.schemas": [StreamRaw, StreamSummary]},
    )

    generated = "\n".join(files.values())

    assert '.withWatermark("event_time", ' in generated
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
