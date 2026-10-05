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

from examples.streams.transforms.foreach_batch_alerts import (
    Alert,
    AlertMessage,
    Event,
    PrepareAlertBatch,
    PublishAlerts,
)

pytestmark: pytest.MarkDecorator | list[pytest.MarkDecorator] = (
    [pytest.mark.integration, pytest.mark.skip(reason="foreachBatch handoff requires classic PySpark")]
    if backend_name().startswith("spark-connect")
    else pytest.mark.integration
)

SOURCE_MODULE = "examples.streams.transforms.foreach_batch_alerts"
GENERATED_PACKAGE = "integration_v11_foreach_batch_generated"
LIFECYCLE_TOKENS = ("writeStream", "foreachBatch", "checkpointLocation", ".start(")


def test_schema_sink_runs_batch_transform_and_caller_deduplicates_replays(spark, tmp_path) -> None:
    files = render_generated_projects(
        (
            (PublishAlerts, f"{SOURCE_MODULE}.PublishAlerts"),
            (PrepareAlertBatch, f"{SOURCE_MODULE}.PrepareAlertBatch"),
        ),
        generated_package=GENERATED_PACKAGE,
        source_schema_modules={SOURCE_MODULE: [Event, Alert, AlertMessage]},
    )
    for path, generated_source in files.items():
        if "/pyspark/transforms/" in path:
            for token in LIFECYCLE_TOKENS:
                assert token not in generated_source

    root = Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"v11-batch-{uuid4().hex}"
    try:
        with generated_project(tmp_path, GENERATED_PACKAGE, files):
            for mode in ("online", "generated"):
                source = root / mode / "source"
                source.mkdir(parents=True)
                (source / "events.json").write_text(
                    json.dumps({"event_id": "e-1", "message": "Gate 4 alert"}) + "\n",
                    encoding="utf-8",
                )
                events = spark.readStream.schema("event_id STRING NOT NULL, message STRING NOT NULL").json(str(source))
                parent = session(spark, execution_mode=mode, generated_package=GENERATED_PACKAGE)
                result = PublishAlerts(events=events).run(parent)
                handoff = result.send_alerts
                assert handoff.dataframe is result.alerts
                assert handoff.schema is AlertMessage

                commits: dict[tuple[str, int], list[dict[str, str]]] = {}

                def commit(stream_id: str, batch_id: int, messages: list[dict[str, str]]) -> None:
                    # The caller-owned test destination atomically keeps the first commit for each key.
                    commits.setdefault((stream_id, batch_id), messages)

                def send_batch(batch_df, batch_id: int) -> None:
                    with parent.spawn() as batch_session:
                        prepared = PrepareAlertBatch(alerts=batch_df).run_batch(batch_session, handoff)
                        messages = [row.asDict() for row in prepared.messages.orderBy("event_id").collect()]
                    # A callback retry presents the same key; this destination deduplicates it.
                    commit("alerts-v1", batch_id, messages)
                    commit("alerts-v1", batch_id, messages)

                query = handoff.dataframe.writeStream.foreachBatch(send_batch).option(
                    "checkpointLocation", str(root / mode / "checkpoint")
                ).trigger(availableNow=True).start()
                try:
                    assert query.awaitTermination(120), "foreachBatch query did not complete"
                finally:
                    query.stop()
                    parent.close()

                assert len(commits) == 1
                assert next(iter(commits.values())) == [{"event_id": "e-1", "payload": "Gate 4 alert"}]
    finally:
        shutil.rmtree(root, ignore_errors=True)
