from __future__ import annotations

import bisect
import json
import shutil
import tempfile
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from integration.pyspark.support.backend_matrix import backend_name

pytestmark: pytest.MarkDecorator | list[pytest.MarkDecorator] = (
    [
        pytest.mark.integration,
        pytest.mark.skip(reason="Python grouped streaming state prototype runs on ordinary Spark only"),
    ]
    if backend_name().startswith("spark-connect")
    else pytest.mark.integration
)


@pytest.mark.parametrize(
    ("maximum", "duplicate", "active_requests", "overflow", "scope_count"),
    [
        (0, False, 1, False, 1),
        (1, False, 1, False, 1),
        (3, False, 1, False, 1),
        (3, False, 16, False, 1),
        (3, False, 1, False, 4),
        (1000, False, 1, False, 1),
        (1, True, 1, False, 1),
        (3, False, 1, True, 1),
    ],
)
def test_i09282601_bounded_request_state_emits_result_after_idle_grace(
    spark, maximum: int, duplicate: bool, active_requests: int, overflow: bool, scope_count: int
) -> None:
    """Prototype bounded per-request ranking and processing-time finalization."""

    import pandas as pd  # type: ignore[import-untyped]
    from pyspark.sql import functions as F
    from pyspark.sql.streaming.state import GroupStateTimeout
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    request_id = f"issue-i09282601-k{maximum}-n{active_requests}"
    request_ids = [f"{request_id}-{index}" for index in range(active_requests)]
    scope_keys = [(active_request, "experiment-0", "band-0") for active_request in request_ids]
    if scope_count > 1:
        scope_keys = [
            (request_ids[0], f"experiment-{index // 2}", f"band-{index % 2}") for index in range(scope_count)
        ]
    grace_seconds = 5
    maximum_unique_candidates = 10_000
    now = datetime.now(UTC)
    deadline = now + timedelta(seconds=60)
    expired_deadline = now - timedelta(seconds=1)
    shared_root = Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration"
    shared_root.mkdir(parents=True, exist_ok=True)
    test_root = Path(tempfile.mkdtemp(prefix="issue-i09282601-", dir=shared_root))
    rows_path = test_root / "request_rows"
    rows_path.mkdir()
    candidate_count = 10_001 if overflow else maximum + 100
    scores = range(candidate_count, 0, -1) if maximum else (None,)

    def write_batch(
        name: str,
        batch_scores: Sequence[tuple[str | None, float | None]],
        row_deadline: datetime = deadline,
    ) -> None:
        (rows_path / name).write_text(
            "\n".join(
                json.dumps(
                    {
                        "request_id": active_request,
                        "experiment_id": experiment_id,
                        "band_id": band_id,
                        "document_id": document_id,
                        "score": score,
                        "request_deadline": row_deadline.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                    }
                )
                for active_request, experiment_id, band_id in scope_keys
                for document_id, score in batch_scores
            ),
            encoding="utf-8",
        )

    base_candidates: list[tuple[str | None, float | None]] = [
        (f"d-{score:04d}", float(score)) for score in scores if score is not None
    ] or [(None, None)]
    if maximum:
        tie_score = float(candidate_count + 1)
        base_candidates.extend((("tie-z", tie_score), ("tie-a", tie_score)))
    write_batch("batch-1.json", base_candidates)

    input_schema = StructType(
        [
            StructField("request_id", StringType(), nullable=False),
            StructField("experiment_id", StringType(), nullable=False),
            StructField("band_id", StringType(), nullable=False),
            StructField("document_id", StringType(), nullable=True),
            StructField("score", DoubleType(), nullable=True),
            StructField("request_deadline", TimestampType(), nullable=False),
        ]
    )
    output_schema = StructType(
        [
            StructField("request_id", StringType(), nullable=False),
            StructField("experiment_id", StringType(), nullable=False),
            StructField("band_id", StringType(), nullable=False),
            StructField("rank", IntegerType(), nullable=False),
            StructField("document_id", StringType(), nullable=True),
            StructField("score", DoubleType(), nullable=True),
            StructField("is_final", BooleanType(), nullable=False),
        ]
    )
    state_schema = StructType(
        [
            StructField("payload_json", StringType(), nullable=False),
            StructField("closed", BooleanType(), nullable=False),
        ]
    )

    def top_k(key, batches, state):
        if state.hasTimedOut:
            payload_json, closed = state.get
            if closed:
                state.remove()
                return iter(())
            state.update(("{}", True))
            state.setTimeoutDuration(10_000)
            candidates = [tuple(candidate) for candidate in json.loads(payload_json).get("candidates", [])]
            final_rows = [
                {
                    "request_id": key[0],
                    "experiment_id": key[1],
                    "band_id": key[2],
                    "rank": rank,
                    "document_id": candidate[1],
                    "score": -candidate[0],
                    "is_final": True,
                }
                for rank, candidate in enumerate(candidates, start=1)
            ] or [
                {
                    "request_id": key[0],
                    "experiment_id": key[1],
                    "band_id": key[2],
                    "rank": 0,
                    "document_id": None,
                    "score": None,
                    "is_final": True,
                }
            ]
            return iter((pd.DataFrame(final_rows),))

        payload = {} if not state.exists else json.loads(state.get[0])
        candidates = [tuple(candidate) for candidate in payload.get("candidates", [])]
        seen_ids = set(payload.get("seen_ids", []))
        closed = False if not state.exists else state.get[1]
        if closed:
            return iter(())
        for batch in batches:
            for _, _, _, document_id, score, _ in batch.itertuples(index=False, name=None):
                if document_id is not None:
                    if document_id in seen_ids:
                        raise ValueError(f"Duplicate candidate key for scope {key!r}: {document_id!r}")
                    seen_ids.add(document_id)
                    if len(seen_ids) > maximum_unique_candidates:
                        raise ValueError(
                            f"Request exceeds the declared candidate bound of {maximum_unique_candidates}."
                        )
                    if maximum:
                        bisect.insort(candidates, (-float(score), document_id))
                        if len(candidates) > maximum:
                            candidates.pop()
        state.update((json.dumps({"candidates": candidates, "seen_ids": sorted(seen_ids)}), False))
        state.setTimeoutDuration(grace_seconds * 1_000)
        return iter(())

    source = spark.readStream.schema(input_schema).json(str(rows_path))
    live_rows = source.where(F.col("request_deadline") > F.current_timestamp())
    result = live_rows.groupBy("request_id", "experiment_id", "band_id").applyInPandasWithState(
        top_k,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="Append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )
    query_name = f"issue_i09282601_{maximum}_{time.monotonic_ns()}"
    query = (
        result.writeStream.format("memory")
        .queryName(query_name)
        .outputMode("append")
        .option("checkpointLocation", str(test_root / "checkpoint"))
        .trigger(processingTime="200 milliseconds")
        .start()
    )
    try:
        observed = []
        wait_until = time.monotonic() + 35
        late_batch_written = False
        while time.monotonic() < wait_until:
            observed = [row.asDict() for row in spark.table(query_name).collect()]
            failure = query.exception()
            if duplicate or overflow:
                if failure is not None:
                    expected_error = "Duplicate candidate key" if duplicate else "declared candidate bound"
                    assert expected_error in str(failure)
                    return
            elif failure is not None:
                raise AssertionError(f"State prototype failed: {failure}")
            progress = query.lastProgress
            if maximum and not overflow and not late_batch_written and progress is not None:
                if progress["numInputRows"] > 0:
                    batch_two = [("d-0001", -1.0)] if duplicate else [("a-late", float(maximum + 1000))]
                    write_batch("batch-2.json", batch_two)
                    late_batch_written = True
            finalized_scopes = {
                (row["request_id"], row["experiment_id"], row["band_id"])
                for row in observed
                if row["is_final"]
            }
            if finalized_scopes == set(scope_keys):
                break
            time.sleep(0.2)
        if duplicate or overflow:
            failed_case = "duplicate candidate" if duplicate else "candidate-bound overflow"
            pytest.fail(f"The bounded state operation did not reject {failed_case}.")
        assert observed, "Idle requests did not receive final results before the deadline."
        expected_per_scope = maximum or 1
        assert len(observed) == expected_per_scope * len(scope_keys)
        expected_by_scope: dict[tuple[str, str, str], list[tuple[int, str]]] = {}
        if maximum:
            from pyspark.sql import Window

            batch_oracle = spark.read.schema(input_schema).json(str(rows_path))
            ranking = Window.partitionBy("request_id", "experiment_id", "band_id").orderBy(
                F.col("score").desc_nulls_last(), F.col("document_id").asc_nulls_first()
            )
            oracle_rows = (
                batch_oracle.where(F.col("request_deadline") > F.current_timestamp())
                .where(F.col("document_id").isNotNull())
                .withColumn("rank", F.row_number().over(ranking))
                .where(F.col("rank") <= maximum)
                .collect()
            )
            for row in oracle_rows:
                scope_key = (row["request_id"], row["experiment_id"], row["band_id"])
                expected_by_scope.setdefault(scope_key, []).append((row["rank"], row["document_id"]))
        rows_by_scope = {
            scope_key: [
                row
                for row in observed
                if (row["request_id"], row["experiment_id"], row["band_id"]) == scope_key
            ]
            for scope_key in scope_keys
        }
        for scope_key, request_rows in rows_by_scope.items():
            if maximum:
                actual = [row["document_id"] for row in sorted(request_rows, key=lambda row: row["rank"])]
                expected = [document_id for _, document_id in sorted(expected_by_scope[scope_key])]
                if actual != expected:
                    pytest.fail(
                        f"Scope {scope_key!r} expected {len(expected)} ranked IDs but received {len(actual)}; "
                        f"missing={sorted(set(expected) - set(actual))[:5]!r}; "
                        f"unexpected={sorted(set(actual) - set(expected))[:5]!r}; "
                        f"actual_prefix={actual[:5]!r}; expected_prefix={expected[:5]!r}"
                    )
            else:
                assert len(request_rows) == 1
                assert request_rows[0]["document_id"] is None
        assert {row["is_final"] for row in observed} == {True}
        write_batch("batch-late.json", [("after-final", float(maximum + 2000))], expired_deadline)
        time.sleep(1)
        assert [row.asDict() for row in spark.table(query_name).collect()] == observed
    finally:
        query.stop()
        shutil.rmtree(test_root, ignore_errors=True)
