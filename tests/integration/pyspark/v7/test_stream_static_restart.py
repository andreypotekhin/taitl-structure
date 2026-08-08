from __future__ import annotations

import json
import shutil
from collections.abc import Callable
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
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.plugin.pyspark import drop_duplicates, exists, inner_join, left_join, string, timestamp, watermark, where

pytestmark: pytest.MarkDecorator | list[pytest.MarkDecorator] = (
    [
        pytest.mark.integration,
        pytest.mark.skip(reason="Structured Streaming restart evidence requires classic PySpark"),
    ]
    if backend_name().startswith("spark-connect")
    else pytest.mark.integration
)

SOURCE_MODULE = "integration.pyspark.v7.test_stream_static_restart"
PACKAGE = "integration_v7_stream_static_generated"
LIFECYCLE_TOKENS = (
    "readStream",
    "writeStream",
    "checkpoint",
    ".trigger(",
    ".outputMode(",
    ".start(",
    "awaitTermination",
    ".collect(",
    ".count(",
    ".toPandas(",
    ".rdd",
    "foreachBatch",
)


class StreamEvent(Schema):
    event_id = string(nullable=False)
    account_id = string(nullable=False)
    event_time = timestamp(nullable=False)


class Account(Schema):
    account_id = string(nullable=False)
    tier = string(nullable=False)


class RequiredEnrichment(Schema):
    event_id = string(nullable=False)
    tier = string(nullable=False)


class OptionalEnrichment(Schema):
    event_id = string(nullable=False)
    tier = string(nullable=True)


class KeptEvent(Schema):
    event_id = string(nullable=False)


@transform(streaming=True)
class StreamStaticInnerEnrichment(Transform):
    events = input(StreamEvent, streaming=True)
    accounts = input(Account)
    enriched = output(RequiredEnrichment)

    def enrich(self, event: StreamEvent, account: Account) -> RequiredEnrichment:
        inner_join(account, on=account.account_id == event.account_id)
        return RequiredEnrichment(event_id=event.event_id, tier=account.tier)


@transform(streaming=True)
class StreamStaticLeftEnrichment(Transform):
    events = input(StreamEvent, streaming=True)
    accounts = input(Account)
    enriched = output(OptionalEnrichment)

    def enrich(self, event: StreamEvent, account: Account) -> OptionalEnrichment:
        left_join(account, on=account.account_id == event.account_id)
        return OptionalEnrichment(event_id=event.event_id, tier=account.tier)


@transform(streaming=True)
class StreamStaticSemiFilter(Transform):
    events = input(StreamEvent, streaming=True)
    accounts = input(Account)
    kept = output(KeptEvent)

    def keep(self, event: StreamEvent, account: Account) -> KeptEvent:
        where(exists(account, on=account.account_id == event.account_id))
        return KeptEvent(event_id=event.event_id)


@transform(streaming=True)
class StreamStatefulStaticLeftEnrichment(Transform):
    events = input(StreamEvent, streaming=True)
    accounts = input(Account)
    enriched = output(OptionalEnrichment)

    def enrich(self, event: StreamEvent, account: Account) -> OptionalEnrichment:
        watermark(event.event_time, delay="1 hour")
        drop_duplicates(event.event_id, event.event_time)
        left_join(account, on=account.account_id == event.account_id)
        return OptionalEnrichment(event_id=event.event_id, tier=account.tier)


def test_v7_stream_static_enrichment_restarts_with_caller_checkpoint(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("Structured Streaming restart evidence is classic PySpark only")

    transform_types: tuple[type[Transform], ...] = (
        StreamStaticInnerEnrichment,
        StreamStaticLeftEnrichment,
        StreamStaticSemiFilter,
    )
    files = render_generated_projects(
        tuple((transform_type, f"{SOURCE_MODULE}.{transform_type.__name__}") for transform_type in transform_types),
        generated_package=PACKAGE,
        source_schema_modules={
            SOURCE_MODULE: [
                StreamEvent,
                Account,
                RequiredEnrichment,
                OptionalEnrichment,
                KeptEvent,
            ],
        },
    )
    _assert_no_lifecycle_calls(files)

    stream_root = Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"v7-{uuid4().hex}"
    try:
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schemas = import_module(f"{PACKAGE}.pyspark.schemas.test_stream_static_restart")
            accounts = spark.createDataFrame(
                [("a-1", "gold"), ("a-2", "silver")],
                schemas.ACCOUNT_SCHEMA,
            )
            source = stream_root / "events"

            _assert_restarts(
                spark,
                source,
                stream_root / "inner-checkpoint",
                stream_root / "inner-output",
                StreamStaticInnerEnrichment,
                lambda stream: StreamStaticInnerEnrichment(events=stream, accounts=accounts)
                .run(session(spark, execution_mode="online"))
                .enriched,
                [
                    {"event_id": "e-1", "tier": "gold"},
                ],
                [
                    {"event_id": "e-3", "tier": "silver"},
                ],
                schemas.STREAM_EVENT_SCHEMA,
            )
            _assert_restarts(
                spark,
                source,
                stream_root / "left-checkpoint",
                stream_root / "left-output",
                StreamStaticLeftEnrichment,
                lambda stream: StreamStaticLeftEnrichment(events=stream, accounts=accounts)
                .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
                .enriched,
                [
                    {"event_id": "e-1", "tier": "gold"},
                    {"event_id": "e-2", "tier": None},
                ],
                [
                    {"event_id": "e-3", "tier": "silver"},
                    {"event_id": "e-4", "tier": None},
                ],
                schemas.STREAM_EVENT_SCHEMA,
            )
            _assert_restarts(
                spark,
                source,
                stream_root / "semi-checkpoint",
                stream_root / "semi-output",
                StreamStaticSemiFilter,
                lambda stream: StreamStaticSemiFilter(events=stream, accounts=accounts)
                .run(session(spark, execution_mode="online"))
                .kept,
                [
                    {"event_id": "e-1"},
                ],
                [
                    {"event_id": "e-3"},
                ],
                schemas.STREAM_EVENT_SCHEMA,
            )
    finally:
        shutil.rmtree(stream_root, ignore_errors=True)


def test_v7_stream_static_left_outer_lookup_restarts_online_and_generated(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("Structured Streaming restart evidence is classic PySpark only")

    files = render_generated_projects(
        ((StreamStaticLeftEnrichment, f"{SOURCE_MODULE}.StreamStaticLeftEnrichment"),),
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [StreamEvent, Account, OptionalEnrichment]},
    )
    _assert_no_lifecycle_calls(files)

    stream_root = Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"v7-{uuid4().hex}"
    try:
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schemas = import_module(f"{PACKAGE}.pyspark.schemas.test_stream_static_restart")
            accounts = spark.createDataFrame(
                [("a-1", "gold"), ("a-2", "silver")],
                schemas.ACCOUNT_SCHEMA,
            )
            source = stream_root / "events"
            expected_first: list[dict[str, object]] = [
                {"event_id": "e-1", "tier": "gold"},
                {"event_id": "e-2", "tier": None},
            ]
            expected_second: list[dict[str, object]] = [
                {"event_id": "e-3", "tier": "silver"},
                {"event_id": "e-4", "tier": None},
            ]

            _assert_restarts(
                spark,
                source / "online",
                stream_root / "left-online-checkpoint",
                stream_root / "left-online-output",
                StreamStaticLeftEnrichment,
                lambda stream: StreamStaticLeftEnrichment(events=stream, accounts=accounts)
                .run(session(spark, execution_mode="online"))
                .enriched,
                expected_first,
                expected_second,
                schemas.STREAM_EVENT_SCHEMA,
            )
            _assert_restarts(
                spark,
                source / "generated",
                stream_root / "left-generated-checkpoint",
                stream_root / "left-generated-output",
                StreamStaticLeftEnrichment,
                lambda stream: StreamStaticLeftEnrichment(events=stream, accounts=accounts)
                .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
                .enriched,
                expected_first,
                expected_second,
                schemas.STREAM_EVENT_SCHEMA,
            )
    finally:
        shutil.rmtree(stream_root, ignore_errors=True)


def test_v7_stateful_stream_static_left_outer_lookup_restarts_online_and_generated(spark, tmp_path) -> None:
    if backend_name().startswith("spark-connect"):
        pytest.skip("Structured Streaming restart evidence is classic PySpark only")

    files = render_generated_projects(
        ((StreamStatefulStaticLeftEnrichment, f"{SOURCE_MODULE}.StreamStatefulStaticLeftEnrichment"),),
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [StreamEvent, Account, OptionalEnrichment]},
    )
    _assert_no_lifecycle_calls(files)

    stream_root = Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"v7-{uuid4().hex}"
    try:
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schemas = import_module(f"{PACKAGE}.pyspark.schemas.test_stream_static_restart")
            accounts = spark.createDataFrame(
                [("a-1", "gold"), ("a-2", "silver")],
                schemas.ACCOUNT_SCHEMA,
            )
            expected_first: list[dict[str, object]] = [
                {"event_id": "e-1", "tier": "gold"},
                {"event_id": "e-2", "tier": None},
            ]
            expected_second: list[dict[str, object]] = [
                {"event_id": "e-3", "tier": "silver"},
            ]

            for mode in ("online", "generated"):
                _assert_stateful_restarts(
                    spark,
                    stream_root / f"events-{mode}",
                    stream_root / f"stateful-{mode}-checkpoint",
                    stream_root / f"stateful-{mode}-output",
                    _stateful_enrichment_frame(spark, accounts, mode),
                    expected_first,
                    expected_second,
                    schemas.STREAM_EVENT_SCHEMA,
                )
    finally:
        shutil.rmtree(stream_root, ignore_errors=True)


def _assert_restarts(
    spark,
    source: Path,
    checkpoint: Path,
    sink: Path,
    transform_type: type[Transform],
    build_frame: Callable[[object], Any],
    first_expected: list[dict[str, object]],
    second_expected: list[dict[str, object]],
    schema,
) -> None:
    transform_source = source / transform_type.__name__
    _write_json(transform_source / "batch-1.json", [_event("e-1", "a-1"), _event("e-2", "missing")])
    first = _run_once(spark, transform_source, checkpoint, sink, build_frame, schema)
    _write_json(transform_source / "batch-2.json", [_event("e-3", "a-2"), _event("e-4", "missing")])
    second = _run_once(spark, transform_source, checkpoint, sink, build_frame, schema)

    assert first == first_expected
    assert second == [*first_expected, *second_expected]


def _assert_stateful_restarts(
    spark,
    source: Path,
    checkpoint: Path,
    sink: Path,
    build_frame: Callable[[object], Any],
    first_expected: list[dict[str, object]],
    second_expected: list[dict[str, object]],
    schema,
) -> None:
    _write_json(source / "batch-1.json", [_event("e-1", "a-1"), _event("e-2", "missing")])
    first = _run_once(spark, source, checkpoint, sink, build_frame, schema)
    _write_json(source / "batch-2.json", [_event("e-1", "a-1"), _event("e-3", "a-2")])
    second = _run_once(spark, source, checkpoint, sink, build_frame, schema)

    assert first == first_expected
    assert second == [*first_expected, *second_expected]


def _stateful_enrichment_frame(spark, accounts, execution_mode: str) -> Callable[[object], Any]:
    def build(stream: object) -> Any:
        return (
            StreamStatefulStaticLeftEnrichment(events=stream, accounts=accounts)
            .run(session(spark, execution_mode=execution_mode, generated_package=PACKAGE))
            .enriched
        )

    return build


def _run_once(
    spark,
    source: Path,
    checkpoint: Path,
    sink: Path,
    build_frame: Callable[[object], Any],
    schema,
) -> list[dict[str, object]]:
    stream = spark.readStream.schema(schema).json(str(source))
    frame = build_frame(stream)
    query = (
        frame.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint))
        .start(str(sink))
    )
    try:
        query.processAllAvailable()
    finally:
        query.stop()
    return rows(spark.read.parquet(str(sink)), "event_id")


def _assert_no_lifecycle_calls(files: dict[str, str]) -> None:
    for path, source in files.items():
        if "/pyspark/transforms/" not in path:
            continue
        for token in LIFECYCLE_TOKENS:
            assert token not in source, f"{path} contains streaming lifecycle token {token!r}"


def _write_json(path: Path, values: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(value) for value in values), encoding="utf-8")


def _event(event_id: str, account_id: str) -> dict[str, object]:
    times = {
        "e-1": "2026-01-01 00:00:00",
        "e-2": "2026-01-01 00:01:00",
        "e-3": "2026-01-01 00:02:00",
        "e-4": "2026-01-01 00:03:00",
    }
    return {"event_id": event_id, "account_id": account_id, "event_time": times[event_id]}
