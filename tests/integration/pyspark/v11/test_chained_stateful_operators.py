from __future__ import annotations

import json
import shutil
from datetime import datetime
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

from structure import Schema, Transform, input, output, transform
from structure.plugin.pyspark import *

pytestmark = pytest.mark.integration

PACKAGE = "chained_stateful_generated"


class ChainEvent(Schema):
    event_id = string(nullable=False)
    event_time = timestamp(nullable=False)


class ChainSummary(Schema):
    bucket = struct(TimeWindow, nullable=False)
    row_count = long(nullable=False)


@transform(streaming=True)
class DedupeThenWindow(Transform):
    events = input(ChainEvent, streaming=True)
    summary = output(ChainSummary)
    memory_budget = budget(memory_source="prefer_spark", fallback_mb=768)

    def summarize(self, row: ChainEvent) -> ChainSummary:
        watermark(row.event_time, delay="10 minutes")
        drop_duplicates_within_watermark(row.event_id)
        budget(max_rows=50_000, max_state_bytes=64 * 1024 * 1024)
        group_by(bucket=window(row.event_time, "10 minutes"))
        budget(max_rows=100_000, max_state_bytes=128 * 1024 * 1024)
        return ChainSummary(bucket=window(row.event_time, "10 minutes"), row_count=count())


def test_v11_dedupe_then_window_runs_online_and_generated(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("chained stateful operator evidence requires an ordinary SparkSession")
    files = render_generated_project(
        DedupeThenWindow,
        source_transform=f"{__name__}.DedupeThenWindow",
        generated_package=PACKAGE,
        source_schema_modules={__name__: [ChainEvent, ChainSummary]},
    )
    source = (
        Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"chain-{uuid4().hex}"
    )
    first_batch = [
        ("duplicate", "2026-01-01T10:01:00Z"),
        ("distinct", "2026-01-01T10:03:00Z"),
    ]
    second_batch = [
        ("duplicate", "2026-01-01T10:02:00Z"),
        ("advance-1", "2026-01-01T10:30:00Z"),
        ("advance-2", "2026-01-01T10:50:00Z"),
    ]
    third_batch = [("advance-3", "2026-01-01T11:10:00Z")]
    online_rows = []
    generated_rows = []
    try:
        source.mkdir(parents=True, exist_ok=True)
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schema = import_module(f"{PACKAGE}.pyspark.schemas.{__name__.rsplit('.', 1)[-1]}")
            input_schema = schema.CHAIN_EVENT_SCHEMA
            for mode, generated in (("online", False), ("generated", True)):
                input_path = source / ("generated-input" if generated else "online-input")
                input_path.mkdir(parents=True, exist_ok=True)
                stream = spark.readStream.schema(input_schema).json(str(input_path))
                invocation = DedupeThenWindow(events=stream)
                result = invocation.run(
                    session(
                        spark,
                        execution_mode=mode,
                        generated_package=PACKAGE if generated else None,
                    )
                )
                batch_rows: list[dict[str, Any]] = []

                def collect_batch(batch, _batch_id):
                    batch_rows.extend(row.asDict(recursive=True) for row in batch.collect())

                frame = result.summary
                checkpoint = str(source / f"checkpoint-{mode}")
                guard = session(
                    spark,
                    execution_mode=mode,
                    generated_package=PACKAGE if generated else None,
                ).state_budget_guard(result)
                for index, events in enumerate((first_batch, second_batch, third_batch)):
                    (input_path / f"batch-{index}.json").write_text(
                        "\n".join(
                            json.dumps({"event_id": event_id, "event_time": at}) for event_id, at in events
                        ),
                        encoding="utf-8",
                    )
                    query = (
                        frame.writeStream.foreachBatch(collect_batch)
                        .outputMode("append")
                        .option("checkpointLocation", checkpoint)
                        .start()
                    )
                    try:
                        guard.attach(query)
                        query.processAllAvailable()
                        guard.check()
                    finally:
                        guard.close()
                        query.stop()
                rows = batch_rows
                rows.sort(key=lambda row: row["bucket"]["start"])
                if generated:
                    generated_rows = rows
                else:
                    online_rows = rows
    finally:
        shutil.rmtree(source, ignore_errors=True)

    assert online_rows == generated_rows
    assert any(
        row["bucket"]["start"] == datetime(2026, 1, 1, 10, 0) and row["row_count"] == 2
        for row in online_rows
    )
