import sys

from structure import (
    AsOf,
    Join,
    JoinDedupe,
    Long,
    String,
    Structure,
    Transform,
    after,
    as_of_one,
    count,
    exists,
    field,
    group_by,
    inner_join,
    input,
    lookup_join,
    not_exists,
    output,
    temporal_one,
    transform,
    where,
)
from structure.app.compiler.api import Compiler
from structure.app.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class StreamRaw(Structure):
    id = field(String(), nullable=False)


class StreamClean(Structure):
    id = field(String(), nullable=False)


class StreamLookup(Structure):
    id = field(String(), nullable=False, primary_key=True)
    value = field(String(), nullable=True)
    valid_from = field(String(), nullable=False)
    valid_to = field(String(), nullable=True)


class StreamEnriched(Structure):
    id = field(String(), nullable=False)
    value = field(String(), nullable=True)


class StreamSummary(Structure):
    id = field(String(), nullable=False)
    row_count = field(Long(), nullable=False)


@transform(streaming_compatible=True)
class StreamingProjection(Transform):
    rows = input(StreamRaw)
    clean = output(StreamClean)

    def normalize(self, row: StreamRaw) -> StreamClean:
        where(row.id.is_not_null())  # type: ignore[attr-defined]
        return StreamClean(id=row.id)


@transform(streaming_compatible=True)
class StreamingUnknownHook(Transform):
    rows = input(StreamRaw)
    clean = output(StreamClean)

    def normalize(self, row: StreamRaw) -> StreamClean:
        return StreamClean(id=row.id)

    @after(normalize, lane=rows)
    def arbitrary_hook(self, *, rows, spark, ctx):
        return rows


@transform(streaming_compatible=True)
class StreamingExists(Transform):
    rows = input(StreamRaw)
    lookups = input(StreamLookup)
    clean = output(StreamClean)

    def keep_known(self, row: StreamRaw, lookup: StreamLookup) -> StreamClean:
        where(exists(on=lookup.id == row.id))
        where(not_exists(on=lookup.value == row.id))
        return StreamClean(id=row.id)


@transform(streaming_compatible=True)
class StreamingJoinMany(Transform):
    rows = input(StreamRaw)
    lookups = input(StreamLookup)
    enriched = output(StreamEnriched)

    def expand(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        inner_join(on=lookup.id == row.id)
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming_compatible=True)
class StreamingDedupedLookup(Transform):
    rows = input(StreamRaw)
    lookups = input(StreamLookup)
    enriched = output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        lookup_join(lookup, on=lookup.id == row.id, how=Join.LEFT, dedupe=JoinDedupe.latest_by(lookup.valid_from))
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming_compatible=True)
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


@transform(streaming_compatible=True)
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


@transform(streaming_compatible=True)
class StreamingAggregate(Transform):
    rows = input(StreamRaw)
    summary = output(StreamSummary)

    def summarize(self, row: StreamRaw) -> StreamSummary:
        return group_by(row.id).agg(row_count=count()).as_schema(StreamSummary)


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
    assert "watermarks" in report.findings[0].problem


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
    assert finding.operation == "after hook arbitrary_hook"
    assert finding.to_diagnostic().docs == "docs/Diagnostics.md#stream-w0801"


def test_v1_streaming_report_is_included_in_explain_output() -> None:
    from structure.app.cli.api import CliApp

    report = CliApp.render_explain_report()(StreamingUnknownHook)

    assert "streaming:" in report
    assert "status: unknown" in report
    assert "required: true" in report
    assert "STREAM-W0801: unknown in normalize (after hook arbitrary_hook)" in report


def test_v2_aggregate_streaming_report_is_included_in_explain_output() -> None:
    from structure.app.cli.api import CliApp

    report = CliApp.render_explain_report()(StreamingAggregate)

    assert "status: batch_only" in report
    assert "STREAM-E0801: batch_only in summarize (grouped aggregate)" in report
    assert "operations: aggregate(aggregate keys=id metrics=count)" in report


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
