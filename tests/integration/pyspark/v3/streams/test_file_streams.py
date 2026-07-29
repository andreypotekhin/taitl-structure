from __future__ import annotations

import json
import shutil
from datetime import date as calendar_date
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
from structure import Schema, Transform, input, output, special, transform
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


@transform(streaming=True)
class StreamingScalarUdf(Transform):
    rows = input(StreamUdfRaw, streaming=True)
    clean = output(StreamUdfClean)

    @special(type="udf", return_type=types.string(), nullable=False)
    def normalize(value: Any):
        return value.strip()

    def normalize_rows(self, row: StreamUdfRaw) -> StreamUdfClean:
        return StreamUdfClean(id=self.normalize(row.id))


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


def _collect_stream(frame, checkpoint, *, output_mode: str, order_by: str) -> list[dict[str, object]]:
    name = f"streams_{uuid4().hex}"
    query = start_memory_query(frame, query_name=name, checkpoint=checkpoint, output_mode=output_mode)
    try:
        query.processAllAvailable()
        return rows(frame.sparkSession.table(name), order_by)
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
