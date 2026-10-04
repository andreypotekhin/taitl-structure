from __future__ import annotations

import json
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_projects,
    session,
)

from examples.streams.foreach_sinks import JsonLinesAlertWriter
from structure import Schema, Transform, input, output, sink, special, step
from structure.plugin.pyspark import foreach, string

pytestmark: pytest.MarkDecorator | list[pytest.MarkDecorator] = (
    [pytest.mark.integration, pytest.mark.skip(reason="row foreach writer evidence requires classic PySpark")]
    if backend_name().startswith("spark-connect")
    else pytest.mark.integration
)

SOURCE_MODULE = "integration.pyspark.v11.test_row_foreach_handoff"
GENERATED_PACKAGE = "integration_v11_foreach_generated"


class StreamEvent(Schema):
    event_id = string(nullable=False)


class PublishedEvent(Schema):
    event_id = string(nullable=False)


class PublishStream(Transform):
    events = input(StreamEvent, streaming=True)
    published = output(PublishedEvent)
    row_sink = sink(JsonLinesAlertWriter)

    @step(output=published)
    def publish(self, event: StreamEvent, sink: JsonLinesAlertWriter) -> PublishedEvent:
        published = PublishedEvent(event_id=event.event_id)
        foreach(published, sink)
        return published


def test_row_foreach_handoff_runs_as_an_independent_streaming_query(spark, tmp_path) -> None:
    files = render_generated_projects(
        ((PublishStream, f"{SOURCE_MODULE}.PublishStream"),),
        generated_package=GENERATED_PACKAGE,
        source_schema_modules={SOURCE_MODULE: [StreamEvent, PublishedEvent]},
    )
    for generated_source in files.values():
        for token in ("foreach(", "foreachBatch(", "writeStream", ".start(", "checkpointLocation"):
            assert token not in generated_source

    root = Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"v11-foreach-{uuid4().hex}"
    with generated_project(tmp_path, GENERATED_PACKAGE, files):
        for mode in ("online", "generated"):
            source = root / mode / "source"
            source.mkdir(parents=True)
            (source / "events.json").write_text(
                json.dumps({"event_id": "e-1"}) + "\n" + json.dumps({"event_id": "e-2"}) + "\n",
                encoding="utf-8",
            )
            events = spark.readStream.schema("event_id STRING NOT NULL").json(str(source))
            result = PublishStream(events=events).run(
                session(spark, execution_mode=mode, generated_package=GENERATED_PACKAGE)
            )
            handoff = result.row_sink
            assert handoff.dataframe is result.published
            assert handoff.writer is JsonLinesAlertWriter

            primary = handoff.dataframe.writeStream.format("parquet").option(
                "path", str(root / mode / "primary")
            ).option("checkpointLocation", str(root / mode / "primary-checkpoint")).trigger(
                availableNow=True
            ).start()
            secondary = handoff.dataframe.writeStream.foreach(
                handoff.writer(destination=str(root / mode / "foreach-data"))
            ).option("checkpointLocation", str(root / mode / "foreach-checkpoint")).trigger(
                availableNow=True
            ).start()
            try:
                assert primary.awaitTermination(120), "primary output query did not complete"
                assert secondary.awaitTermination(120), "foreach query did not complete"
                records = [
                    json.loads(line)
                    for path in (root / mode / "foreach-data").glob("*.jsonl")
                    for line in path.read_text(encoding="utf-8").splitlines()
                ]
                assert sum(record["event"] == "process" for record in records) == 2
                assert any(record["event"] == "open" for record in records)
                assert any(record["event"] == "close" for record in records)
            finally:
                primary.stop()
                secondary.stop()
                shutil.rmtree(root / mode, ignore_errors=True)


def test_batch_foreach_receives_rows_only_after_the_caller_runs_the_action(spark) -> None:
    count = spark.sparkContext.accumulator(0)

    class BatchWriter:
        def __init__(self, value) -> None:
            self.value = value

        def process(self, row) -> None:
            self.value.add(1)

    dataframe = spark.createDataFrame([(1,), (2,)], "value INT")
    dataframe.foreach(BatchWriter(count).process)

    assert count.value == 2
