import json
import sys
from pathlib import Path
from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.core.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.plugin.api.v1.model import BackendCapabilityError
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.types import StructType
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody

ROOT = Path(__file__).resolve().parents[3]
V9_LEDGER = ROOT / "src/structure/plugin/pyspark/resources/pyspark-streaming-api-coverage.json"


def _compile(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False)


def _compile_with_plugin(transform, profile: str):
    return Compiler.frontend.compile()(
        transform,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": profile, "variant": "ordinary"}},
    )


def _analysis(transform):
    return _compile(transform).analysis


def _recipe(transform) -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, _compile(transform).lowered)


def _body(transform) -> PySparkStepBody:
    return cast(PySparkStepBody, _analysis(transform).steps[0].plugin_body)


def _v9_ledger_entry(api_id: str) -> dict[str, Any]:
    entries = json.loads(V9_LEDGER.read_text(encoding="utf-8"))["entries"]
    return next(entry for entry in entries if entry["id"] == api_id)


class StreamRaw(Schema):
    id = string(nullable=False)
    event_time = timestamp(nullable=False)


class StreamRawWithNote(Schema):
    id = string(nullable=False)
    event_time = timestamp(nullable=False)
    note = string()


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


class StreamRequiredLookupEnriched(Schema):
    id = string(nullable=False)
    value = string(nullable=False)


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


@transform(streaming=True)
class StreamingFirstWindow(Transform):
    rows = input(StreamRaw, streaming=True)
    summary = output(StreamWindowSummary)

    def summarize(self, row: StreamRaw) -> StreamWindowSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(bucket=window(row.event_time, "10 minutes"), id=row.id)
        return StreamWindowSummary(
            bucket=window(row.event_time, "10 minutes"),
            id=row.id,
            row_count=count(),
        )


@transform(streaming=True)
class StreamingSecondWindow(Transform):
    windows = input(StreamWindowSummary, streaming=True)
    summary = output(StreamWindowSummary)

    def summarize(self, row: StreamWindowSummary) -> StreamWindowSummary:
        group_by(bucket=window(window_time(row.bucket), "1 hour"), id=row.id)
        return StreamWindowSummary(
            bucket=window(window_time(row.bucket), "1 hour"),
            id=row.id,
            row_count=count(),
        )


class StreamGlobalWindowSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    row_count = long(nullable=False)


class StreamTerm(Schema):
    token = string(nullable=False)
    weight = integer(nullable=False)


class StreamDocument(Schema):
    id = string(nullable=False)
    terms = array(struct(StreamTerm), contains_null=False, nullable=False)


class StreamExplodedTerm(Schema):
    token = string(nullable=False)
    weight = integer(nullable=False)


class StreamDocumentTerm(Schema):
    id = string(nullable=False)
    token = string(nullable=False)
    weight = integer(nullable=False)


class StreamRanked(Schema):
    id = string(nullable=False)
    rank = long(nullable=False)


class StreamingVariantInput(Schema):
    id = string(nullable=False)
    event_time = timestamp(nullable=False)
    payload = variant(nullable=True)
    payload_json = string(nullable=True)
    attributes = map(string(), string(), nullable=True)


class StreamingVariantOutput(Schema):
    id = string(nullable=False)
    parsed = variant(nullable=True)
    safe_parsed = variant(nullable=True)
    schema = string(nullable=True)
    name = string(nullable=True)
    safe_name = string(nullable=True)
    object = variant(nullable=True)
    is_json_null = boolean(nullable=True)


class StreamingVariantValidOutput(Schema):
    id = string(nullable=False)
    is_valid = boolean(nullable=True)


class StreamingVariantSchemaSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    id = string(nullable=False)
    schema = string(nullable=True)


class StreamingVariantExplodeEntry(Schema):
    pos = long(nullable=False)
    key = string(nullable=True)
    value = variant(nullable=False)


class StreamingVariantExplodeOutput(Schema):
    id = string(nullable=False)
    pos = long(nullable=False)
    key = string(nullable=True)
    item = string(nullable=True)


class StreamingVariantExplodeOuterEntry(Schema):
    pos = long(nullable=True)
    key = string(nullable=True)
    value = variant(nullable=True)


class StreamingVariantExplodeOuterOutput(Schema):
    id = string(nullable=False)
    pos = long(nullable=True)
    key = string(nullable=True)
    item = string(nullable=True)


@transform(streaming=True)
class StreamingVariantHelpers(Transform):
    rows = input(StreamingVariantInput, streaming=True)
    variants = output(StreamingVariantOutput)

    def convert(self, row: StreamingVariantInput) -> StreamingVariantOutput:
        return StreamingVariantOutput(
            id=row.id,
            parsed=parse_json(row.payload_json),
            safe_parsed=try_parse_json(row.payload_json),
            schema=schema_of_variant(row.payload),
            name=variant_get(row.payload, "$.name", as_type=types.string()),
            safe_name=try_variant_get(row.payload, "$.name", as_type=types.string()),
            object=to_variant_object(row.attributes),
            is_json_null=is_variant_null(row.payload),
        )


@transform(streaming=True)
class StreamingVariantValidation(Transform):
    rows = input(StreamingVariantInput, streaming=True)
    variants = output(StreamingVariantValidOutput)

    def validate(self, row: StreamingVariantInput) -> StreamingVariantValidOutput:
        return StreamingVariantValidOutput(id=row.id, is_valid=is_valid_variant(row.payload))


@transform(streaming=True)
class StreamingVariantSchemaAggregate(Transform):
    rows = input(StreamingVariantInput, streaming=True)
    summary = output(StreamingVariantSchemaSummary)

    def summarize(self, row: StreamingVariantInput) -> StreamingVariantSchemaSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(bucket=window(row.event_time, "10 minutes"), id=row.id)
        return StreamingVariantSchemaSummary(
            bucket=window(row.event_time, "10 minutes"),
            id=row.id,
            schema=schema_of_variant_agg(row.payload),
        )


@transform(streaming=True)
class StreamingVariantExplode(Transform):
    rows = input(StreamingVariantInput, streaming=True)
    expanded = output(StreamingVariantExplodeOutput)

    def expand(self, row: StreamingVariantInput) -> StreamingVariantExplodeOutput:
        entry = variant_explode(row.payload, as_=StreamingVariantExplodeEntry)
        return StreamingVariantExplodeOutput(
            id=row.id,
            pos=entry.pos,
            key=entry.key,
            item=variant_get(entry.value, "$", as_type=types.string()),
        )


@transform(streaming=True)
class StreamingVariantExplodeOuter(Transform):
    rows = input(StreamingVariantInput, streaming=True)
    expanded = output(StreamingVariantExplodeOuterOutput)

    def expand(self, row: StreamingVariantInput) -> StreamingVariantExplodeOuterOutput:
        entry = variant_explode_outer(row.payload, as_=StreamingVariantExplodeOuterEntry)
        return StreamingVariantExplodeOuterOutput(
            id=row.id,
            pos=entry.pos,
            key=entry.key,
            item=variant_get(entry.value, "$", as_type=types.string()),
        )


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


@transform(streaming=True)
class StreamingStructGenerator(Transform):
    documents = input(StreamDocument, streaming=True)
    terms = output(StreamDocumentTerm)

    def expand(self, document: StreamDocument) -> StreamDocumentTerm:
        term = explode_struct(document.terms, as_=StreamExplodedTerm, scope="term")
        return StreamDocumentTerm(id=document.id, token=term.token, weight=term.weight)


@transform(streaming=True)
class StreamingUnionAll(Transform):
    rows = input(StreamRaw, streaming=True)
    more_rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def merge(self, row: StreamRaw, more: StreamRaw) -> StreamClean:
        merged = union_all(more)
        return StreamClean(id=merged.id)


@transform(streaming=True)
class StreamingUnionByName(Transform):
    rows = input(StreamRaw, streaming=True)
    more_rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def merge(self, row: StreamRaw, more: StreamRaw) -> StreamClean:
        merged = union_by_name(more)
        return StreamClean(id=merged.id)


@transform(streaming=True)
class StreamingUnionByNameMissingColumns(Transform):
    rows = input(StreamRawWithNote, streaming=True)
    more_rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def merge(self, row: StreamRawWithNote, more: StreamRaw) -> StreamClean:
        merged = union_by_name(more, allow_missing_columns=True)
        return StreamClean(id=merged.id)


@transform(streaming=True)
class StreamingUnionStaticSide(Transform):
    rows = input(StreamRaw, streaming=True)
    more_rows = input(StreamRaw)
    clean = output(StreamClean)

    def merge(self, row: StreamRaw, more: StreamRaw) -> StreamClean:
        merged = union_all(more)
        return StreamClean(id=merged.id)


@transform(streaming=True)
class StreamingIntersect(Transform):
    rows = input(StreamRaw, streaming=True)
    more_rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def merge(self, row: StreamRaw, more: StreamRaw) -> StreamClean:
        merged = intersect(more)
        return StreamClean(id=merged.id)


@transform(streaming=True)
class StreamingOrderLimitOffset(Transform):
    rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def rank(self, row: StreamRaw) -> StreamClean:
        order_by(row.event_time.desc(), row.id)
        limit(10)
        page = offset(2)
        return StreamClean(id=page.id)


@transform(streaming=True)
class StreamingPrioritySelection(Transform):
    rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def pick(self, row: StreamRaw) -> StreamClean:
        selected = select_first_qualified(
            row.id,
            where=row.id.is_not_null(),  # type: ignore[attr-defined]
            order_by=row.event_time.asc(),
        )
        return StreamClean(id=selected.id)


@transform(streaming=True)
class StreamingSelectedRows(Transform):
    rows = input(StreamRaw, streaming=True)
    clean = output(StreamClean)

    def latest(self, row: StreamRaw) -> StreamClean:
        latest_by(row.event_time, partition_by=row.id)
        return StreamClean(id=row.id)


@transform(streaming=True)
class StreamingWindowProjection(Transform):
    rows = input(StreamRaw, streaming=True)
    ranked = output(StreamRanked)

    def rank(self, row: StreamRaw) -> StreamRanked:
        return StreamRanked(
            id=row.id,
            rank=row_number(partition_by=row.id, order_by=row.event_time),
        )


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
    rows = input(StreamRaw, streaming=True)
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
class StreamingStaticLeftOuterLookup(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup)
    enriched = output(StreamEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        left_join(lookup, on=lookup.id == row.id)
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingStaticLeftOuterLookupRequiredField(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup)
    enriched = output(StreamRequiredLookupEnriched)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamRequiredLookupEnriched:
        left_join(lookup, on=lookup.id == row.id)
        return StreamRequiredLookupEnriched(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingStaticRightJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup)
    enriched = output(StreamOuter)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamOuter:
        right_join(lookup, on=lookup.id == row.id)
        return StreamOuter(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingStaticFullJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup)
    enriched = output(StreamOuter)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamOuter:
        full_join(lookup, on=lookup.id == row.id)
        return StreamOuter(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingStaticCrossJoin(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup)
    enriched = output(StreamOuter)

    def enrich(self, row: StreamRaw, lookup: StreamLookup) -> StreamOuter:
        cross_join(lookup, allow_cartesian=True)
        return StreamOuter(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingDedupedLookup(Transform):
    rows = input(StreamRaw, streaming=True)
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
    rows = input(StreamRaw, streaming=True)
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
    rows = input(StreamRaw, streaming=True)
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
    rows = input(StreamRaw, streaming=True)
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
class StreamingDedupeThenStaticLeftEnrichment(Transform):
    rows = input(StreamRaw, streaming=True)
    lookups = input(StreamLookup)
    enriched = output(StreamEnriched)

    def unique_enriched_rows(self, row: StreamRaw, lookup: StreamLookup) -> StreamEnriched:
        watermark(row.event_time, delay="10 minutes")
        drop_duplicates(row.id)
        left_join(lookup, on=lookup.id == row.id)
        where(row.id.is_not_null())  # type: ignore[attr-defined]
        return StreamEnriched(id=row.id, value=lookup.value)


@transform(streaming=True)
class StreamingDedupeThenAggregate(Transform):
    rows = input(StreamRaw, streaming=True)
    summary = output(StreamWindowSummary)

    def summarize(self, row: StreamRaw) -> StreamWindowSummary:
        watermark(row.event_time, delay="10 minutes")
        drop_duplicates(row.id)
        group_by(bucket=window(row.event_time, "10 minutes"), id=row.id)
        return StreamWindowSummary(bucket=window(row.event_time, "10 minutes"), id=row.id, row_count=count())


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


def test_v8_struct_generator_is_streaming_compatible_without_spark() -> None:
    report = Compiler.compileability.streaming()(_recipe(StreamingStructGenerator), required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v9_variant_helpers_are_profile_gated_streaming_transforms_without_spark() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        _compile_with_plugin(StreamingVariantHelpers, ">=3.5,<4.0")

    assert raised.value.diagnostic.feature_group == "schema"
    assert raised.value.diagnostic.feature_name == "variant"

    plan = cast(PySparkExecutionPlan, _compile_with_plugin(StreamingVariantHelpers, ">=4.0,<4.1").lowered)
    report = Compiler.compileability.streaming()(plan, required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v9_variant_4_2_helper_is_profile_gated_for_streaming_without_spark() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        _compile_with_plugin(StreamingVariantValidation, ">=4.0,<4.1")

    assert raised.value.diagnostic.feature_group == "expression"
    assert raised.value.diagnostic.feature_name == "is_valid_variant"

    plan = cast(PySparkExecutionPlan, _compile_with_plugin(StreamingVariantValidation, ">=4.2,<4.3").lowered)
    report = Compiler.compileability.streaming()(plan, required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v9_variant_schema_aggregate_follows_watermarked_streaming_rules_without_spark() -> None:
    plan = cast(PySparkExecutionPlan, _compile_with_plugin(StreamingVariantSchemaAggregate, ">=4.0,<4.1").lowered)
    report = Compiler.compileability.streaming()(plan, required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v9_variant_tvf_is_profile_gated_and_streaming_compatible_without_spark() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        _compile_with_plugin(StreamingVariantExplode, ">=3.5,<4.0")

    assert raised.value.diagnostic.feature_group == "schema"
    assert raised.value.diagnostic.feature_name == "variant"

    plan = cast(PySparkExecutionPlan, _compile_with_plugin(StreamingVariantExplode, ">=4.0,<4.1").lowered)
    report = Compiler.compileability.streaming()(plan, required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v9_variant_outer_tvf_is_streaming_compatible_without_spark() -> None:
    plan = cast(PySparkExecutionPlan, _compile_with_plugin(StreamingVariantExplodeOuter, ">=4.0,<4.1").lowered)
    report = Compiler.compileability.streaming()(plan, required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


@pytest.mark.parametrize("transform_type", [StreamingUnionAll, StreamingUnionByName])
def test_v8_stream_stream_union_is_compatible_without_spark(transform_type: type[Transform]) -> None:
    report = Compiler.compileability.streaming()(_recipe(transform_type), required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v8_union_requires_both_relations_declared_streaming() -> None:
    report = Compiler.compileability.streaming()(_recipe(StreamingUnionStaticSide), required=True)

    assert report.support is StreamingSupport.BATCH_ONLY
    assert report.findings[0].operation == "union_all more"
    assert "both exact-schema relations are declared with streaming=True" in report.findings[0].problem


def test_v9_missing_column_union_by_name_is_batch_only_for_streaming() -> None:
    report = Compiler.compileability.streaming()(_recipe(StreamingUnionByNameMissingColumns), required=True)

    assert report.support is StreamingSupport.BATCH_ONLY
    assert report.findings[0].operation == "union_by_name more"
    assert "allow_missing_columns=True" in report.findings[0].problem


def test_v8_distinct_style_relation_sets_remain_streaming_ineligible() -> None:
    report = Compiler.compileability.streaming()(_recipe(StreamingIntersect), required=True)

    assert report.support is StreamingSupport.BATCH_ONLY
    assert report.findings[0].operation == "intersect more"
    assert "not compatible" in report.findings[0].problem


def test_v9_stateful_and_order_sensitive_gap_diagnostics_match_api_ledger() -> None:
    cases = [
        (StreamingIntersect, "streaming.distinct-style-sets", "intersect more", "not compatible"),
        (StreamingOrderLimitOffset, "streaming.ordering-bounds", "order_by", "batch materialization boundary"),
        (
            StreamingPrioritySelection,
            "streaming.priority-selection",
            "select_first_qualified",
            "perform priority selection before the streaming transform",
        ),
        (StreamingSelectedRows, "streaming.selected-row-helpers", "latest-row selection", "move selected-row streaming state"),
        (StreamingWindowProjection, "streaming.analytic-windows", "window projection", "move streaming window state"),
        (
            StreamingDedupeThenAggregate,
            "streaming.stateful-composition",
            "stateful streaming composition",
            "split any later stateful work",
        ),
    ]

    for transform_type, ledger_id, operation, guidance in cases:
        report = Compiler.compileability.streaming()(_recipe(transform_type), required=True)
        entry = _v9_ledger_entry(ledger_id)

        assert report.support is StreamingSupport.BATCH_ONLY
        assert any(finding.operation == operation for finding in report.findings), ledger_id
        assert any(guidance in f"{finding.problem} {finding.use}" for finding in report.findings), ledger_id
        assert entry["status"] in {"structure-supported", "streaming-ineligible", "design-gated"}
        assert entry["support_claim"] in {
            "transformed-dataframe-partial",
            "no-streaming-support",
            "no-structure-support",
            "compile-time-state-policy",
        }


def test_v2_stream_static_analytical_joins_are_compatible_without_spark() -> None:
    for transform_type in (StreamingExists, StreamingJoinMany):
        plan = _analysis(transform_type)
        report = Compiler.compileability.streaming()(
            PySpark.compiler.lower()(plan),
            required=bool((plan.options or {})["streaming"]),
        )

        assert report.support is StreamingSupport.COMPATIBLE
        assert report.findings == ()


def test_v7_stream_static_left_outer_lookup_is_compatible_without_spark() -> None:
    plan = _analysis(StreamingStaticLeftOuterLookup)
    report = Compiler.compileability.streaming()(PySpark.compiler.lower()(plan), required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v7_stream_static_left_outer_lookup_requires_nullable_lookup_fields() -> None:
    with pytest.raises(StructureCompileError) as raised:
        _compile(StreamingStaticLeftOuterLookupRequiredField)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"
    assert raised.value.diagnostic.context == {"field": "value", "schema": "StreamRequiredLookupEnriched"}


@pytest.mark.parametrize(
    "transform_type",
    [
        StreamingStaticRightJoin,
        StreamingStaticFullJoin,
        StreamingStaticCrossJoin,
    ],
)
def test_v7_stream_static_reverse_and_broad_outer_joins_are_batch_only(transform_type: type[Transform]) -> None:
    plan = _analysis(transform_type)
    report = Compiler.compileability.streaming()(PySpark.compiler.lower()(plan), required=True)

    assert report.support is StreamingSupport.BATCH_ONLY
    assert report.findings[0].operation == "rowset join lookup"
    assert "streaming state" in report.findings[0].problem


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


def test_v2_streaming_marker_does_not_make_batch_lookup_join_batch_only() -> None:
    @transform(streaming=True)
    class BatchLookup(Transform):
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

    report = Compiler.compileability.streaming()(_recipe(BatchLookup), required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v2_business_key_grouped_aggregates_follow_pyspark_output_modes() -> None:
    plan = _analysis(StreamingAggregate)

    report = Compiler.compileability.streaming()(
        PySpark.compiler.lower()(plan),
        required=bool((plan.options or {})["streaming"]),
    )

    assert report.support is StreamingSupport.COMPATIBLE
    assert len(report.findings) == 1
    assert report.findings[0].code == "STREAM-W0802"
    operation = next(operation for operation in _body(StreamingAggregate).operations if operation.aggregate is not None)
    assert operation.streaming_output_modes == (StreamingOutputMode.UPDATE, StreamingOutputMode.COMPLETE)


def test_frontend_compile_attaches_streaming_warning_for_unbounded_aggregate() -> None:
    compilation = cast(Any, Compiler.frontend.compile()(StreamingAggregate, materialize_schemas=False))

    assert "STREAM-W0802" in {diagnostic.code for diagnostic in compilation.analysis.diagnostics}


def test_frontend_compile_attaches_required_streaming_diagnostic_for_stream_input() -> None:
    compilation = cast(Any, Compiler.frontend.compile()(StreamingOneSidedStreamJoin, materialize_schemas=False))

    assert "STREAM-E0801" in {diagnostic.code for diagnostic in compilation.analysis.diagnostics}


def test_v4_watermarked_business_key_aggregate_follows_pyspark_output_modes() -> None:
    unbounded = _analysis(StreamingWatermarkedBusinessKeyAggregate)
    report = Compiler.compileability.streaming()(PySpark.compiler.lower()(unbounded), required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert len(report.findings) == 1
    assert report.findings[0].code == "STREAM-W0802"
    operation = next(
        operation for operation in _body(StreamingWatermarkedBusinessKeyAggregate).operations if operation.aggregate is not None
    )
    assert operation.streaming_output_modes == (StreamingOutputMode.UPDATE, StreamingOutputMode.COMPLETE)


def test_v4_event_time_window_has_time_window_schema_and_rejects_mixed_signature() -> None:
    aggregate = next(operation.aggregate for operation in _body(StreamingWatermarkedAggregate).operations if operation.aggregate is not None)

    assert aggregate is not None
    assert isinstance(aggregate.keys[0].expression.type, StructType)
    assert aggregate.keys[0].expression.type.schema is TimeWindow
    with pytest.raises(TypeError, match="cannot mix event-time arguments"):
        window("event_time", "10 minutes", partition_by="id")  # type: ignore[call-overload]


def test_v9_window_time_requires_a_time_window_value() -> None:
    with pytest.raises(TypeError, match="requires a TimeWindow value"):
        window_time(StreamRaw.event_time)  # type: ignore[attr-defined]


@pytest.mark.parametrize("profile", [">=3.5,<4.0", ">=4.0,<4.1"])
def test_v9_chained_event_time_windows_are_streaming_compatible_without_spark(profile: str) -> None:
    pipeline = StreamingFirstWindow(rows=object()).to(StreamingSecondWindow())
    compiled = _compile_with_plugin(pipeline, profile)
    plan = cast(PySparkExecutionPlan, compiled.lowered)

    report = Compiler.compileability.streaming()(plan, required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()
    second_aggregate = next(operation.aggregate for operation in plan.steps[1].operations if operation.aggregate)
    assert second_aggregate.keys[0].expression.kind == "time_window"
    assert second_aggregate.keys[0].expression.args[0].kind == "window_time"


def test_v9_chained_event_time_windows_render_window_time_without_lifecycle_calls() -> None:
    pipeline = StreamingFirstWindow(rows=object()).to(StreamingSecondWindow())
    plan = cast(PySparkExecutionPlan, _compile_with_plugin(pipeline, ">=4.0,<4.1").lowered)
    rendered = "\n".join(
        PySpark.render.project()(
            plan,
            source_transform="tests.specifications.streaming_compatibility.StreamingWindowRollup",
            generated_package="streaming_generated",
            source_schema_modules={__name__: [StreamRaw, StreamWindowSummary]},
        ).values()
    )

    assert "F.window_time(" in rendered
    assert "readStream" not in rendered
    assert "writeStream" not in rendered


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


def test_v7_single_stateful_operation_can_feed_static_enrichment_without_spark() -> None:
    report = Compiler.compileability.streaming()(_recipe(StreamingDedupeThenStaticLeftEnrichment), required=True)

    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()


def test_v7_second_admitted_stateful_operation_is_batch_only_without_spark() -> None:
    report = Compiler.compileability.streaming()(_recipe(StreamingDedupeThenAggregate), required=True)

    assert report.support is StreamingSupport.BATCH_ONLY
    assert report.findings[-1].operation == "stateful streaming composition"
    assert "watermark-bounded duplicate removal" in report.findings[-1].problem
    assert "watermark-bounded event-time aggregate" in report.findings[-1].problem


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
    assert "caller-owned PySpark code" in finding.use
    assert "readStream" in finding.use
    assert "foreach side effects" in finding.use


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

    assert "status: compatible" in report
    assert "operations: aggregate(aggregate keys=id metrics=count streaming_modes=update|complete)" in report


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


@pytest.mark.parametrize(
    ("transform_type", "schemas"),
    [
        (StreamingStructGenerator, [StreamTerm, StreamDocument, StreamExplodedTerm, StreamDocumentTerm]),
        (StreamingUnionAll, [StreamRaw, StreamClean]),
        (StreamingUnionByName, [StreamRaw, StreamClean]),
    ],
)
def test_v8_generated_stateless_gap_code_avoids_streaming_lifecycle_and_actions(
    transform_type: type[Transform],
    schemas: list[type[Schema]],
) -> None:
    files = PySpark.render.project()(
        _recipe(transform_type),
        source_transform=f"tests.fixtures.streaming.transforms.{transform_type.__name__}",
        generated_package="streaming_generated",
        source_schema_modules={"tests.fixtures.streaming.schemas": schemas},
    )

    generated = "\n".join(files.values())

    forbidden = (
        "readStream",
        "writeStream",
        ".trigger(",
        ".outputMode(",
        ".start(",
        "awaitTermination",
        "checkpoint",
        "collect(",
        "count(",
        "toPandas",
        ".rdd",
        "foreachBatch",
    )
    assert all(value not in generated for value in forbidden)
