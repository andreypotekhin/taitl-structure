from __future__ import annotations

import json
import shutil
from datetime import date as calendar_date
from datetime import datetime as calendar_datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.rows import rows

from examples.streams.adoption import read_json_stream, start_memory_query, stop_query
from examples.streams.schemas.events import GateProgress, JudgeCall, Passage, Penalty, RawEvent
from examples.streams.schemas.race import Gate, Paddler, Race
from examples.streams.transforms.passages import PreparePassages
from examples.streams.transforms.penalties import CorrelatePenalties
from examples.streams.transforms.progress import BuildGateProgress
from structure import Schema, Transform, input, output, special, stage, transform
from structure.plugin.api.v1.model import BackendCapabilityError
from structure.plugin.pyspark import *

pytestmark = pytest.mark.integration

PACKAGE = "integration_streams_generated"
SCHEMA_MODULES = {
    "examples.streams.schemas.events": [RawEvent, Passage, JudgeCall, GateProgress, Penalty],
    "examples.streams.schemas.race": [Race, Gate, Paddler],
}


class StreamUdfRaw(Schema):
    id = string(nullable=False)


class StreamUdfClean(Schema):
    id = string(nullable=False)


class StreamVariantRaw(Schema):
    id = string(nullable=False)
    payload_json = string(nullable=True)
    attributes = map(string(), string(), nullable=True)


class StreamVariantClean(Schema):
    id = string(nullable=False)
    parsed = variant(nullable=True)
    safe_parsed = variant(nullable=True)
    schema = string(nullable=True)
    name = string(nullable=True)
    safe_name = string(nullable=True)
    object = variant(nullable=True)
    is_json_null = boolean(nullable=True)
    literal_source = string(nullable=True)


class StreamVariantExplodeRaw(Schema):
    id = string(nullable=False)
    payload_json = string(nullable=True)


class StreamVariantExplodeEntry(Schema):
    pos = long(nullable=False)
    key = string(nullable=True)
    value = variant(nullable=False)


class StreamVariantExplodeOutput(Schema):
    id = string(nullable=False)
    pos = long(nullable=False)
    key = string(nullable=True)
    item = string(nullable=True)


class StreamVariantExplodeOuterEntry(Schema):
    pos = long(nullable=True)
    key = string(nullable=True)
    value = variant(nullable=True)


class StreamVariantExplodeOuterOutput(Schema):
    id = string(nullable=False)
    pos = long(nullable=True)
    key = string(nullable=True)
    item = string(nullable=True)


class StreamVariantAggregateRaw(Schema):
    id = string(nullable=False)
    event_time = timestamp(nullable=False)
    payload_json = string(nullable=True)


class StreamVariantAggregateSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    id = string(nullable=False)
    schema = string(nullable=True)


class StreamWindowRaw(Schema):
    id = string(nullable=False)
    event_time = timestamp(nullable=False)


class StreamWindowSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    id = string(nullable=False)
    row_count = long(nullable=False)


class StreamFiniteSelectionSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    id = string(nullable=False)
    first_event_time = timestamp(nullable=True)
    last_event_time = timestamp(nullable=True)


@transform(streaming=True)
class StreamingFirstWindow(Transform):
    rows = input(StreamWindowRaw, streaming=True)
    summary = output(StreamWindowSummary)

    def summarize(self, row: StreamWindowRaw) -> StreamWindowSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(bucket=window(row.event_time, "10 minutes"), id=row.id)
        return StreamWindowSummary(bucket=window(row.event_time, "10 minutes"), id=row.id, row_count=count())


@transform(streaming=True)
class StreamingSecondWindow(Transform):
    windows = input(StreamWindowSummary, streaming=True)
    summary = output(StreamWindowSummary)

    def summarize(self, row: StreamWindowSummary) -> StreamWindowSummary:
        group_by(bucket=window(window_time(row.bucket), "1 hour"), id=row.id)
        return StreamWindowSummary(bucket=window(window_time(row.bucket), "1 hour"), id=row.id, row_count=count())


@transform(streaming=True)
class StreamingFiniteSelection(Transform):
    rows = input(StreamWindowRaw, streaming=True)
    summary = output(StreamFiniteSelectionSummary)

    def select(self, row: StreamWindowRaw) -> StreamFiniteSelectionSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(bucket=window(row.event_time, "10 minutes"), id=row.id)
        return StreamFiniteSelectionSummary(
            bucket=window(row.event_time, "10 minutes"),
            id=row.id,
            first_event_time=first_value(row.event_time, order_by=row.event_time),
            last_event_time=last_value(row.event_time, order_by=row.event_time),
        )


@transform(streaming=True)
class StreamingWindowRollup(Transform):
    rows = input(StreamWindowRaw, streaming=True)
    summary = output(StreamWindowSummary)

    first = stage(StreamingFirstWindow(rows=rows))
    second = stage(StreamingSecondWindow(windows=first.summary))
    result = output(summary=second.summary)


@transform(streaming=True)
class StreamingScalarUdf(Transform):
    rows = input(StreamUdfRaw, streaming=True)
    clean = output(StreamUdfClean)

    @special(type="udf", return_type=types.string(), nullable=False)
    def normalize(value: Any):
        return value.strip()

    def normalize_rows(self, row: StreamUdfRaw) -> StreamUdfClean:
        return StreamUdfClean(id=self.normalize(row.id))


@transform(streaming=True)
class StreamingVariantHelpers(Transform):
    rows = input(StreamVariantRaw, streaming=True)
    clean = output(StreamVariantClean)

    def convert(self, row: StreamVariantRaw) -> StreamVariantClean:
        parsed = parse_json(row.payload_json)
        return StreamVariantClean(
            id=row.id,
            parsed=parsed,
            safe_parsed=try_parse_json(row.payload_json),
            schema=schema_of_variant(parsed),
            name=variant_get(parsed, "$.name", as_type=types.string()),
            safe_name=try_variant_get(parsed, "$.name", as_type=types.string()),
            object=to_variant_object(row.attributes),
            is_json_null=is_variant_null(parse_json("null")),
            literal_source=variant_get(
                variant_literal('{"source":"literal"}'),
                "$.source",
                as_type=types.string(),
            ),
        )


@transform(streaming=True)
class StreamingVariantExplode(Transform):
    rows = input(StreamVariantExplodeRaw, streaming=True)
    expanded = output(StreamVariantExplodeOutput)

    def expand(self, row: StreamVariantExplodeRaw) -> StreamVariantExplodeOutput:
        entry = variant_explode(parse_json(row.payload_json), as_=StreamVariantExplodeEntry)
        return StreamVariantExplodeOutput(
            id=row.id,
            pos=entry.pos,
            key=entry.key,
            item=variant_get(entry.value, "$", as_type=types.string()),
        )


@transform(streaming=True)
class StreamingVariantExplodeOuter(Transform):
    rows = input(StreamVariantExplodeRaw, streaming=True)
    expanded = output(StreamVariantExplodeOuterOutput)

    def expand(self, row: StreamVariantExplodeRaw) -> StreamVariantExplodeOuterOutput:
        entry = variant_explode_outer(parse_json(row.payload_json), as_=StreamVariantExplodeOuterEntry)
        return StreamVariantExplodeOuterOutput(
            id=row.id,
            pos=entry.pos,
            key=entry.key,
            item=variant_get(entry.value, "$", as_type=types.string()),
        )


@transform(streaming=True)
class StreamingVariantSchemaAggregate(Transform):
    rows = input(StreamVariantAggregateRaw, streaming=True)
    summary = output(StreamVariantAggregateSummary)

    def summarize(self, row: StreamVariantAggregateRaw) -> StreamVariantAggregateSummary:
        watermark(row.event_time, delay="10 minutes")
        group_by(bucket=window(row.event_time, "10 minutes"), id=row.id)
        return StreamVariantAggregateSummary(
            bucket=window(row.event_time, "10 minutes"),
            id=row.id,
            schema=schema_of_variant_agg(parse_json(row.payload_json)),
        )


def test_caller_owned_file_streams_run_online_and_generated_transforms(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("memory sink verification requires a classic PySpark session")

    files = {}
    for transform_type, source in (
        (PreparePassages, "examples.streams.transforms.passages.PreparePassages"),
        (BuildGateProgress, "examples.streams.transforms.progress.BuildGateProgress"),
        (CorrelatePenalties, "examples.streams.transforms.penalties.CorrelatePenalties"),
    ):
        files.update(
            render_generated_project(
                transform_type,
                source_transform=source,
                generated_package=PACKAGE,
                source_schema_modules=SCHEMA_MODULES,
            )
        )

    stream_root = (
        Path(__file__).resolve().parents[5] / ".pytest-workspace-tmp" / "integration" / f"streams-{uuid4().hex}"
    )
    try:
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schemas = import_module(f"{PACKAGE}.pyspark.schemas.events")
            race_schemas = import_module(f"{PACKAGE}.pyspark.schemas.race")
            events_path = stream_root / "events"
            passages_path = stream_root / "passages"
            calls_path = stream_root / "calls"
            _write_json(events_path / "events.json", [_event("e-1"), _event("e-1")])
            _write_json(passages_path / "passages.json", [_passage("e-1")])
            _write_json(calls_path / "calls.json", [_call("c-1")])

            races = spark.createDataFrame(
                [("r-1", "River Run", calendar_date(2026, 7, 12), "Truckee", "Boca", "Sierra", "USA")],
                race_schemas.RACE_SCHEMA,
            )
            paddlers = spark.createDataFrame(
                [("r-1", "p-1", 12, "Ava Stone", "NZL", "women-k1")], race_schemas.PADDLER_SCHEMA
            )
            gates = spark.createDataFrame([("r-1", 4, "upstream", "Narrows")], race_schemas.GATE_SCHEMA)
            event_stream = read_json_stream(spark, schemas.RAW_EVENT_SCHEMA, events_path)
            progress_passages = read_json_stream(spark, schemas.PASSAGE_SCHEMA, passages_path)
            penalty_passages = read_json_stream(spark, schemas.PASSAGE_SCHEMA, passages_path)
            call_stream = read_json_stream(spark, schemas.JUDGE_CALL_SCHEMA, calls_path)
            online_penalty_passages = read_json_stream(spark, schemas.PASSAGE_SCHEMA, passages_path)
            online_call_stream = read_json_stream(spark, schemas.JUDGE_CALL_SCHEMA, calls_path)

            online_passages = (
                PreparePassages(events=event_stream, races=races, paddlers=paddlers, gates=gates)
                .run(session(spark, execution_mode="online"))
                .passages
            )
            generated_progress = (
                BuildGateProgress(passages=progress_passages)
                .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
                .progress
            )
            generated_penalties = (
                CorrelatePenalties(passages=penalty_passages, calls=call_stream)
                .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
                .penalties
            )
            online_penalties = (
                CorrelatePenalties(passages=online_penalty_passages, calls=online_call_stream)
                .run(session(spark, execution_mode="online"))
                .penalties
            )

            passages = _collect_stream(
                online_passages,
                tmp_path / "passage-checkpoint",
                output_mode="append",
                order_by="id",
            )
            progress = _collect_stream(
                generated_progress,
                tmp_path / "progress-checkpoint",
                output_mode="complete",
                order_by="gate_number",
            )
            penalties = _collect_stream(
                generated_penalties,
                tmp_path / "penalty-checkpoint",
                output_mode="append",
                order_by="call_id",
            )
            online_penalties = _collect_stream(
                online_penalties,
                tmp_path / "online-penalty-checkpoint",
                output_mode="append",
                order_by="call_id",
            )
    finally:
        shutil.rmtree(stream_root, ignore_errors=True)

    assert len(passages) == 1
    assert passages[0]["race_name"] == "River Run"
    assert passages[0]["paddler_name"] == "Ava Stone"
    assert passages[0]["gate_direction"] == "upstream"
    assert progress == [
        {
            "race_id": "r-1",
            "run_id": "heat-1",
            "gate_number": 4,
            "passage_count": 1,
            "fastest_millis": 1_000,
            "slowest_millis": 1_000,
        }
    ]
    assert penalties == [
        {
            "event_id": "e-1",
            "call_id": "c-1",
            "race_id": "r-1",
            "run_id": "heat-1",
            "paddler_id": "p-1",
            "gate_number": 4,
            "elapsed_millis": 1_000,
            "penalty_code": "touch",
            "penalty_seconds": 2,
            "adjusted_millis": 3_000,
        }
    ]
    assert online_penalties == penalties


def test_scalar_udf_runs_as_a_row_local_caller_owned_file_stream_transform(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("scalar Python UDFs are ordinary-PySpark only")

    package = "integration_streaming_udf_generated"
    files = render_generated_project(
        StreamingScalarUdf,
        source_transform=f"{__name__}.StreamingScalarUdf",
        generated_package=package,
        source_schema_modules={__name__: [StreamUdfRaw, StreamUdfClean]},
    )
    source = Path(__file__).resolve().parents[5] / ".pytest-workspace-tmp" / "integration" / f"scalar-udf-{uuid4().hex}"
    try:
        _write_json(source / "events.json", [{"id": "  event-1  "}])

        with generated_project(tmp_path, package, files):
            from pyspark.sql.types import StringType, StructField, StructType

            schema = StructType([StructField("id", StringType(), nullable=False)])
            online_input = read_json_stream(spark, schema, source)
            generated_input = read_json_stream(spark, schema, source)
            online = StreamingScalarUdf(rows=online_input).run(session(spark, execution_mode="online")).clean
            generated = (
                StreamingScalarUdf(rows=generated_input)
                .run(session(spark, execution_mode="generated", generated_package=package))
                .clean
            )

            online_rows = _collect_stream(
                online, tmp_path / "online-udf-checkpoint", output_mode="append", order_by="id"
            )
            generated_rows = _collect_stream(
                generated,
                tmp_path / "generated-udf-checkpoint",
                output_mode="append",
                order_by="id",
            )
    finally:
        shutil.rmtree(source, ignore_errors=True)

    assert online_rows == [{"id": "event-1"}]
    assert generated_rows == online_rows


def test_variant_helpers_run_as_profile_gated_streaming_transforms(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("Variant streaming evidence is for the ordinary PySpark runtime")

    if backend_name().endswith("35") or backend_name() == "local":
        with pytest.raises(BackendCapabilityError) as raised:
            render_generated_project(
                StreamingVariantHelpers,
                source_transform=f"{__name__}.StreamingVariantHelpers",
                generated_package="integration_streaming_variant_generated",
                source_schema_modules={__name__: [StreamVariantRaw, StreamVariantClean]},
            )

        assert raised.value.diagnostic.feature_group == "schema"
        assert raised.value.diagnostic.feature_name == "variant"
        return

    if not backend_name().endswith("40"):
        pytest.skip("Variant streaming live evidence currently runs on the PySpark 4.0 integration profile")

    package = "integration_streaming_variant_generated"
    files = render_generated_project(
        StreamingVariantHelpers,
        source_transform=f"{__name__}.StreamingVariantHelpers",
        generated_package=package,
        source_schema_modules={__name__: [StreamVariantRaw, StreamVariantClean]},
    )
    source = Path(__file__).resolve().parents[5] / ".pytest-workspace-tmp" / "integration" / f"variant-{uuid4().hex}"
    try:
        _write_json(
            source / "events.json",
            [
                {
                    "id": "event-1",
                    "payload_json": '{"name":"Ava","score":7}',
                    "attributes": {"source": "stream", "lane": "v9"},
                }
            ],
        )

        with generated_project(tmp_path, package, files):
            from pyspark.sql.types import MapType, StringType, StructField, StructType

            schema = StructType(
                [
                    StructField("id", StringType(), nullable=False),
                    StructField("payload_json", StringType(), nullable=True),
                    StructField("attributes", MapType(StringType(), StringType()), nullable=True),
                ]
            )
            online_input = read_json_stream(spark, schema, source)
            generated_input = read_json_stream(spark, schema, source)
            online = StreamingVariantHelpers(rows=online_input).run(session(spark, execution_mode="online")).clean
            generated = (
                StreamingVariantHelpers(rows=generated_input)
                .run(session(spark, execution_mode="generated", generated_package=package))
                .clean
            )

            online_rows = _collect_stream_projection(
                online,
                tmp_path / "online-variant-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "schema", "name", "safe_name", "is_json_null", "literal_source"),
            )
            generated_rows = _collect_stream_projection(
                generated,
                tmp_path / "generated-variant-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "schema", "name", "safe_name", "is_json_null", "literal_source"),
            )
    finally:
        shutil.rmtree(source, ignore_errors=True)

    assert online_rows == [
        {
            "id": "event-1",
            "schema": "OBJECT<name: STRING, score: BIGINT>",
            "name": "Ava",
            "safe_name": "Ava",
            "is_json_null": True,
            "literal_source": "literal",
        }
    ]
    assert generated_rows == online_rows


def test_variant_explode_runs_online_and_generated_on_pyspark_4_streams(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("Variant TVF evidence is for the ordinary PySpark runtime")

    if backend_name().endswith("35") or backend_name() == "local":
        with pytest.raises(BackendCapabilityError) as raised:
            render_generated_project(
                StreamingVariantExplode,
                source_transform=f"{__name__}.StreamingVariantExplode",
                generated_package="integration_streaming_variant_explode_generated",
                source_schema_modules={
                    __name__: [StreamVariantExplodeRaw, StreamVariantExplodeEntry, StreamVariantExplodeOutput],
                },
            )
        assert raised.value.diagnostic.feature_name == "variant_explode"
        return

    if not backend_name().endswith("40"):
        pytest.skip("Variant TVF live evidence currently runs on the PySpark 4.0 integration profile")

    package = "integration_streaming_variant_explode_generated"
    files = render_generated_project(
        StreamingVariantExplode,
        source_transform=f"{__name__}.StreamingVariantExplode",
        generated_package=package,
        source_schema_modules={
            __name__: [StreamVariantExplodeRaw, StreamVariantExplodeEntry, StreamVariantExplodeOutput],
        },
    )
    source = (
        Path(__file__).resolve().parents[5]
        / ".pytest-workspace-tmp"
        / "integration"
        / f"variant-explode-{uuid4().hex}"
    )
    try:
        _write_json(
            source / "events.json",
            [
                {"id": "event-1", "payload_json": '["a", "b"]'},
                {"id": "event-2", "payload_json": '{"name":"Ava"}'},
            ],
        )

        with generated_project(tmp_path, package, files):
            from pyspark.sql.types import StringType, StructField, StructType

            schema = StructType(
                [
                    StructField("id", StringType(), nullable=False),
                    StructField("payload_json", StringType(), nullable=True),
                ]
            )
            online_input = read_json_stream(spark, schema, source)
            generated_input = read_json_stream(spark, schema, source)
            online = StreamingVariantExplode(rows=online_input).run(session(spark, execution_mode="online")).expanded
            generated = StreamingVariantExplode(rows=generated_input).run(
                session(spark, execution_mode="generated", generated_package=package)
            ).expanded
            online_rows = _collect_stream_projection(
                online,
                tmp_path / "online-variant-explode-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "pos", "key", "item"),
            )
            generated_rows = _collect_stream_projection(
                generated,
                tmp_path / "generated-variant-explode-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "pos", "key", "item"),
            )
    finally:
        shutil.rmtree(source, ignore_errors=True)

    assert online_rows == [
        {"id": "event-1", "pos": 0, "key": None, "item": "a"},
        {"id": "event-1", "pos": 1, "key": None, "item": "b"},
        {"id": "event-2", "pos": 0, "key": "name", "item": "Ava"},
    ]
    assert generated_rows == online_rows


def test_variant_explode_outer_preserves_null_rows_online_and_generated_on_pyspark_4_streams(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("Variant TVF evidence is for the ordinary PySpark runtime")

    if backend_name().endswith("35") or backend_name() == "local":
        with pytest.raises(BackendCapabilityError) as raised:
            render_generated_project(
                StreamingVariantExplodeOuter,
                source_transform=f"{__name__}.StreamingVariantExplodeOuter",
                generated_package="integration_streaming_variant_explode_outer_generated",
                source_schema_modules={
                    __name__: [
                        StreamVariantExplodeRaw,
                        StreamVariantExplodeOuterEntry,
                        StreamVariantExplodeOuterOutput,
                    ],
                },
            )
        assert raised.value.diagnostic.feature_name == "variant_explode_outer"
        return

    if not backend_name().endswith("40"):
        pytest.skip("Variant TVF live evidence currently runs on the PySpark 4.0 integration profile")

    package = "integration_streaming_variant_explode_outer_generated"
    files = render_generated_project(
        StreamingVariantExplodeOuter,
        source_transform=f"{__name__}.StreamingVariantExplodeOuter",
        generated_package=package,
        source_schema_modules={
            __name__: [StreamVariantExplodeRaw, StreamVariantExplodeOuterEntry, StreamVariantExplodeOuterOutput],
        },
    )
    source = (
        Path(__file__).resolve().parents[5]
        / ".pytest-workspace-tmp"
        / "integration"
        / f"variant-explode-outer-{uuid4().hex}"
    )
    try:
        _write_json(
            source / "events.json",
            [
                {"id": "event-1", "payload_json": '["a"]'},
                {"id": "event-2", "payload_json": None},
                {"id": "event-3", "payload_json": '{"name":"Ava"}'},
            ],
        )

        with generated_project(tmp_path, package, files):
            from pyspark.sql.types import StringType, StructField, StructType

            schema = StructType(
                [
                    StructField("id", StringType(), nullable=False),
                    StructField("payload_json", StringType(), nullable=True),
                ]
            )
            online_input = read_json_stream(spark, schema, source)
            generated_input = read_json_stream(spark, schema, source)
            online = StreamingVariantExplodeOuter(rows=online_input).run(
                session(spark, execution_mode="online")
            ).expanded
            generated = StreamingVariantExplodeOuter(rows=generated_input).run(
                session(spark, execution_mode="generated", generated_package=package)
            ).expanded
            online_rows = _collect_stream_projection(
                online,
                tmp_path / "online-variant-explode-outer-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "pos", "key", "item"),
            )
            generated_rows = _collect_stream_projection(
                generated,
                tmp_path / "generated-variant-explode-outer-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "pos", "key", "item"),
            )
    finally:
        shutil.rmtree(source, ignore_errors=True)

    assert online_rows == [
        {"id": "event-1", "pos": 0, "key": None, "item": "a"},
        {"id": "event-2", "pos": None, "key": None, "item": None},
        {"id": "event-3", "pos": 0, "key": "name", "item": "Ava"},
    ]
    assert generated_rows == online_rows


def test_variant_schema_aggregate_runs_with_a_watermarked_window_on_pyspark_4_streams(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("Variant aggregate evidence is for the ordinary PySpark runtime")

    if backend_name().endswith("35") or backend_name() == "local":
        with pytest.raises(BackendCapabilityError) as raised:
            render_generated_project(
                StreamingVariantSchemaAggregate,
                source_transform=f"{__name__}.StreamingVariantSchemaAggregate",
                generated_package="integration_streaming_variant_aggregate_generated",
                source_schema_modules={
                    __name__: [StreamVariantAggregateRaw, StreamVariantAggregateSummary],
                    "structure.plugin.pyspark.dsl.TimeWindow": [TimeWindow],
                },
            )
        assert raised.value.diagnostic.feature_name == "schema_of_variant_agg"
        return

    if not backend_name().endswith("40"):
        pytest.skip("Variant aggregate live evidence currently runs on the PySpark 4.0 integration profile")

    package = "integration_streaming_variant_aggregate_generated"
    files = render_generated_project(
        StreamingVariantSchemaAggregate,
        source_transform=f"{__name__}.StreamingVariantSchemaAggregate",
        generated_package=package,
        source_schema_modules={
            __name__: [StreamVariantAggregateRaw, StreamVariantAggregateSummary],
            "structure.plugin.pyspark.dsl.TimeWindow": [TimeWindow],
        },
    )
    source = (
        Path(__file__).resolve().parents[5]
        / ".pytest-workspace-tmp"
        / "integration"
        / f"variant-aggregate-{uuid4().hex}"
    )
    try:
        _write_json(
            source / "events.json",
            [
                {
                    "id": "event-1",
                    "event_time": "2026-07-12T10:00:00Z",
                    "payload_json": '{"name":"Ava","score":7}',
                }
            ],
        )

        with generated_project(tmp_path, package, files):
            from pyspark.sql.types import StringType, StructField, StructType, TimestampType

            schema = StructType(
                [
                    StructField("id", StringType(), nullable=False),
                    StructField("event_time", TimestampType(), nullable=False),
                    StructField("payload_json", StringType(), nullable=True),
                ]
            )
            online_input = read_json_stream(spark, schema, source)
            generated_input = read_json_stream(spark, schema, source)
            online = StreamingVariantSchemaAggregate(rows=online_input).run(
                session(spark, execution_mode="online")
            ).summary
            generated = StreamingVariantSchemaAggregate(rows=generated_input).run(
                session(spark, execution_mode="generated", generated_package=package)
            ).summary
            online_rows = _collect_stream_projection(
                online,
                tmp_path / "online-variant-aggregate-checkpoint",
                output_mode="complete",
                order_by="id",
                columns=("id", "schema"),
            )
            generated_rows = _collect_stream_projection(
                generated,
                tmp_path / "generated-variant-aggregate-checkpoint",
                output_mode="complete",
                order_by="id",
                columns=("id", "schema"),
            )
    finally:
        shutil.rmtree(source, ignore_errors=True)

    assert online_rows == [{"id": "event-1", "schema": "OBJECT<name: STRING, score: BIGINT>"}]
    assert generated_rows == online_rows


def test_chained_event_time_windows_run_online_and_generated(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("chained window evidence is for the ordinary PySpark runtime")

    package = "integration_streaming_window_generated"
    files = render_generated_project(
        StreamingWindowRollup,
        source_transform=f"{__name__}.StreamingWindowRollup",
        generated_package=package,
        source_schema_modules={
            __name__: [StreamWindowRaw, StreamWindowSummary],
            "structure.plugin.pyspark.dsl.TimeWindow": [TimeWindow],
        },
    )
    source = Path(__file__).resolve().parents[5] / ".pytest-workspace-tmp" / "integration" / f"window-{uuid4().hex}"
    online_source = source / "online"
    generated_source = source / "generated"
    try:
        events = [
            {"id": "event-1", "event_time": "2026-01-01T10:01:00Z"},
            {"id": "event-1", "event_time": "2026-01-01T10:02:00Z"},
            {"id": "watermark-advance", "event_time": "2026-01-01T12:00:00Z"},
        ]
        _write_json(online_source / "events.json", events)
        _write_json(generated_source / "events.json", events)

        with generated_project(tmp_path, package, files):
            from pyspark.sql.types import StringType, StructField, StructType, TimestampType

            schema = StructType(
                [
                    StructField("id", StringType(), nullable=False),
                    StructField("event_time", TimestampType(), nullable=False),
                ]
            )
            online_input = read_json_stream(spark, schema, online_source)
            generated_input = read_json_stream(spark, schema, generated_source)
            online_result = StreamingWindowRollup(rows=online_input).run(session(spark, execution_mode="online")).summary
            generated_result = StreamingWindowRollup(rows=generated_input).run(
                session(spark, execution_mode="generated", generated_package=package)
            ).summary
            online_rows = _collect_stream_projection(
                online_result,
                tmp_path / "online-window-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "row_count"),
            )
            generated_rows = _collect_stream_projection(
                generated_result,
                tmp_path / "generated-window-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "row_count"),
            )
    finally:
        shutil.rmtree(source, ignore_errors=True)

    assert online_rows == [{"id": "event-1", "row_count": 1}]
    assert generated_rows == online_rows


def test_finite_window_selection_runs_online_and_generated(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("finite selected-value evidence is for the ordinary PySpark runtime")

    package = "integration_streaming_finite_selection_generated"
    files = render_generated_project(
        StreamingFiniteSelection,
        source_transform=f"{__name__}.StreamingFiniteSelection",
        generated_package=package,
        source_schema_modules={
            __name__: [StreamWindowRaw, StreamFiniteSelectionSummary],
            "structure.plugin.pyspark.dsl.TimeWindow": [TimeWindow],
        },
    )
    source = (
        Path(__file__).resolve().parents[5]
        / ".pytest-workspace-tmp"
        / "integration"
        / f"finite-selection-{uuid4().hex}"
    )
    online_source = source / "online"
    generated_source = source / "generated"
    try:
        events = [
            {"id": "event-1", "event_time": "2026-01-01T10:02:00Z"},
            {"id": "event-1", "event_time": "2026-01-01T10:01:00Z"},
            {"id": "watermark-advance", "event_time": "2026-01-01T12:00:00Z"},
        ]
        _write_json(online_source / "events.json", events)
        _write_json(generated_source / "events.json", events)

        with generated_project(tmp_path, package, files):
            from pyspark.sql.types import StringType, StructField, StructType, TimestampType

            schema = StructType(
                [
                    StructField("id", StringType(), nullable=False),
                    StructField("event_time", TimestampType(), nullable=False),
                ]
            )
            online_input = read_json_stream(spark, schema, online_source)
            generated_input = read_json_stream(spark, schema, generated_source)
            online = StreamingFiniteSelection(rows=online_input).run(
                session(spark, execution_mode="online")
            ).summary
            generated = StreamingFiniteSelection(rows=generated_input).run(
                session(spark, execution_mode="generated", generated_package=package)
            ).summary
            online_rows = _collect_stream_projection(
                online,
                tmp_path / "online-finite-selection-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "first_event_time", "last_event_time"),
            )
            generated_rows = _collect_stream_projection(
                generated,
                tmp_path / "generated-finite-selection-checkpoint",
                output_mode="append",
                order_by="id",
                columns=("id", "first_event_time", "last_event_time"),
            )
    finally:
        shutil.rmtree(source, ignore_errors=True)

    assert online_rows == [
        {
            "id": "event-1",
            "first_event_time": calendar_datetime(2026, 1, 1, 10, 1),
            "last_event_time": calendar_datetime(2026, 1, 1, 10, 2),
        }
    ]
    assert generated_rows == online_rows


def _collect_stream(frame, checkpoint, *, output_mode: str, order_by: str) -> list[dict[str, object]]:
    name = f"streams_{uuid4().hex}"
    query = start_memory_query(frame, query_name=name, checkpoint=checkpoint, output_mode=output_mode)
    try:
        query.processAllAvailable()
        return rows(frame.sparkSession.table(name), order_by)
    finally:
        stop_query(query)


def _collect_stream_projection(
    frame,
    checkpoint,
    *,
    output_mode: str,
    order_by: str,
    columns: tuple[str, ...],
) -> list[dict[str, object]]:
    name = f"streams_{uuid4().hex}"
    query = start_memory_query(frame, query_name=name, checkpoint=checkpoint, output_mode=output_mode)
    try:
        query.processAllAvailable()
        return rows(frame.sparkSession.table(name).select(*columns), order_by)
    finally:
        stop_query(query)


def _write_json(path, values) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(value) for value in values), encoding="utf-8")


def _event(id: str) -> dict[str, object]:
    return {
        "id": id,
        "race_id": "r-1",
        "run_id": "heat-1",
        "paddler_id": "p-1",
        "gate_number": 4,
        "at": "2026-07-12T10:00:00Z",
        "sequence": 1,
        "elapsed_millis": 1_000,
        "source": "timing-gate-4",
    }


def _call(id: str) -> dict[str, object]:
    return {
        "id": id,
        "race_id": "r-1",
        "run_id": "heat-1",
        "paddler_id": "p-1",
        "gate_number": 4,
        "at": "2026-07-12T10:01:00Z",
        "code": "touch",
        "penalty_seconds": 2,
    }


def _passage(id: str) -> dict[str, object]:
    return _event(id) | {
        "race_name": "River Run",
        "race_date": "2026-07-12",
        "river": "Truckee",
        "venue": "Boca",
        "city": "Sierra",
        "race_country": "USA",
        "paddler_name": "Ava Stone",
        "bib": 12,
        "division": "women-k1",
        "paddler_country": "NZL",
        "gate_direction": "upstream",
        "sector": "Narrows",
    }
