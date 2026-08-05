from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_projects,
    session,
)

from examples.streams.adoption import ForeachBatchSafety, start_foreach_batch_query
from structure import Schema, Transform, input, output, transform
from structure.plugin.pyspark import string

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v10.test_foreach_batch_restart"
PACKAGE = "integration_v10_foreach_batch_generated"
LIFECYCLE_TOKENS = (
    "readStream",
    "writeStream",
    "foreachBatch",
    "checkpoint",
    ".trigger(",
    ".outputMode(",
    ".start(",
    "awaitTermination",
)


class StreamEvent(Schema):
    event_id = string(nullable=False)


class EmittedEvent(Schema):
    event_id = string(nullable=False)


@transform(streaming=True)
class PassThrough(Transform):
    events = input(StreamEvent, streaming=True)
    emitted = output(EmittedEvent)

    def emit(self, event: StreamEvent) -> EmittedEvent:
        return EmittedEvent(event_id=event.event_id)


def test_foreach_batch_sink_restarts_with_caller_checkpoint(spark, tmp_path) -> None:
    """A caller-owned batch sink can resume without generated lifecycle code."""

    if backend_name().startswith("spark-connect"):
        pytest.skip("foreachBatch restart evidence is classic PySpark only")

    files = render_generated_projects(
        ((PassThrough, f"{SOURCE_MODULE}.PassThrough"),),
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [StreamEvent, EmittedEvent]},
    )
    _assert_no_lifecycle_calls(files)

    stream_root = Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"v10-{uuid4().hex}"
    try:
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schemas = import_module(f"{PACKAGE}.pyspark.schemas.test_foreach_batch_restart")
            for mode in ("online", "generated"):
                source = stream_root / mode / "events"
                sink = stream_root / mode / "sink"
                checkpoint = stream_root / mode / "checkpoint"
                _write_json(source / "batch-1.json", [{"event_id": "e-1"}])

                first = _run_once(
                    spark,
                    source,
                    checkpoint,
                    sink,
                    schemas.STREAM_EVENT_SCHEMA,
                    lambda stream: _frame(spark, mode, stream),
                )
                _write_json(source / "batch-2.json", [{"event_id": "e-2"}])
                second = _run_once(
                    spark,
                    source,
                    checkpoint,
                    sink,
                    schemas.STREAM_EVENT_SCHEMA,
                    lambda stream: _frame(spark, mode, stream),
                )

                assert first == [{"event_id": "e-1"}]
                assert second == [{"event_id": "e-1"}, {"event_id": "e-2"}]
    finally:
        shutil.rmtree(stream_root, ignore_errors=True)


def test_foreach_batch_sink_retries_failed_batch_after_restart(spark, tmp_path) -> None:
    """A failed caller callback can be retried from the same checkpoint."""

    if backend_name().startswith("spark-connect"):
        pytest.skip("foreachBatch retry evidence is classic PySpark only")

    files = render_generated_projects(
        ((PassThrough, f"{SOURCE_MODULE}.PassThrough"),),
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [StreamEvent, EmittedEvent]},
    )
    _assert_no_lifecycle_calls(files)

    stream_root = (
        Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"v10-retry-{uuid4().hex}"
    )
    try:
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schemas = import_module(f"{PACKAGE}.pyspark.schemas.test_foreach_batch_restart")
            for mode in ("online", "generated"):
                source = stream_root / mode / "events"
                sink = stream_root / mode / "sink"
                checkpoint = stream_root / mode / "checkpoint"
                _write_json(source / "batch-1.json", [{"event_id": "retry-me"}])

                failed = _run_once(
                    spark,
                    source,
                    checkpoint,
                    sink,
                    schemas.STREAM_EVENT_SCHEMA,
                    lambda stream: _frame(spark, mode, stream),
                    fail_first_batch=True,
                    expect_failure=True,
                )
                resumed = _run_once(
                    spark,
                    source,
                    checkpoint,
                    sink,
                    schemas.STREAM_EVENT_SCHEMA,
                    lambda stream: _frame(spark, mode, stream),
                )

                assert failed == []
                assert resumed == [{"event_id": "retry-me"}]
    finally:
        shutil.rmtree(stream_root, ignore_errors=True)


def _frame(spark, execution_mode: str, stream: Any) -> Any:
    return (
        PassThrough(events=stream).run(session(spark, execution_mode=execution_mode, generated_package=PACKAGE)).emitted
    )


def _run_once(
    spark,
    source: Path,
    checkpoint: Path,
    sink: Path,
    schema: Any,
    build_frame: Any,
    *,
    fail_first_batch: bool = False,
    expect_failure: bool = False,
) -> list[dict[str, object]]:
    stream = spark.readStream.schema(schema).json(str(source))
    frame = build_frame(stream)
    failure_pending = fail_first_batch

    def write_batch(batch_frame: Any, batch_id: int) -> None:
        nonlocal failure_pending
        if failure_pending:
            failure_pending = False
            raise RuntimeError("intentional foreachBatch failure for restart evidence")
        values = [row.asDict(recursive=True) for row in batch_frame.orderBy("event_id").collect()]
        sink.mkdir(parents=True, exist_ok=True)
        (sink / f"batch-{batch_id}.json").write_text(json.dumps(values, sort_keys=True), encoding="utf-8")

    query = start_foreach_batch_query(
        frame,
        write_batch,
        checkpoint=checkpoint,
        output_mode="append",
        safety=ForeachBatchSafety(
            sink_identity=f"v10-foreach-batch:{sink}",
            idempotence_key="snapshot_id:batch_id",
            retry_policy="idempotent",
            snapshot_id="v10-foreach-batch-restart-v1",
        ),
        trigger={"availableNow": True},
    )
    failure: Exception | None = None
    terminated = False
    try:
        terminated = bool(query.awaitTermination(120))
    except Exception as error:
        failure = error
    finally:
        query.stop()

    if expect_failure:
        assert failure is not None, "intentional foreachBatch failure did not terminate the query"
    else:
        assert failure is None, failure
        assert terminated, "availableNow foreachBatch query did not terminate"

    values: list[dict[str, object]] = []
    for batch_file in sorted(sink.glob("batch-*.json")):
        values.extend(json.loads(batch_file.read_text(encoding="utf-8")))
    return values


def _assert_no_lifecycle_calls(files: dict[str, str]) -> None:
    for path, source in files.items():
        if "/pyspark/transforms/" not in path:
            continue
        for token in LIFECYCLE_TOKENS:
            assert token not in source, f"{path} contains streaming lifecycle token {token!r}"


def _write_json(path: Path, values: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(value) for value in values), encoding="utf-8")
