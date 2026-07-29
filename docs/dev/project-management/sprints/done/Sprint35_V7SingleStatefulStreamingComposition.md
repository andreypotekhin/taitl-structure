# Sprint 35: V7 Single-Stateful Streaming Composition

## Sprint Goal

Allow one existing, bounded stateful streaming operation followed only by stateless transformations.

## In Scope

- A design-gated composition of one watermarked dedupe, window/session aggregate, or bounded stream-stream join with downstream stateless projection, filtering, or static enrichment.
- Output-mode inheritance, watermark/state explain records, diagnostics, generated-source scans, and file-stream restart evidence on PySpark 3.5/4.0.

## Out of Scope

- Two-stateful chains, generators, ordering/limits, analytic windows, source/sink ownership, and Spark Connect streaming.

## Acceptance

- The compiler accepts only the proven composition, preserves the upstream operation's output-mode rule, and fails early for a second stateful operation.

## Outcome

- Complete. The PySpark streaming compatibility classifier now admits one already accepted stateful streaming operation
  and rejects a second accepted stateful operation with a corrective `STREAM-E0801` diagnostic.
- Spark-free coverage proves watermarked dedupe followed by stateless filtering and stream-static left enrichment stays
  compatible, while dedupe followed by a watermarked aggregate is batch-only.
- Live restart coverage proves watermarked dedupe plus stream-static left enrichment under caller-owned source, sink,
  checkpoint, output mode, and query lifecycle in both online and generated execution on PySpark 3.5 and 4.0.

## Evidence

- `poetry run pytest tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py` passed with
  44 tests.
- `poetry run pytest tests/integration/pyspark/v7/test_stream_static_restart.py` skipped 3 tests locally as expected
  without live Spark.
- `docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm -e INTEGRATION_PYTEST_ARGS='/workspace/tests/integration/pyspark/v7/test_stream_static_restart.py' structure-integration-pyspark35`
  passed with 6 tests and 3 skips.
- `docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm -e INTEGRATION_PYTEST_ARGS='/workspace/tests/integration/pyspark/v7/test_stream_static_restart.py' structure-integration-pyspark40`
  passed with 9 tests.

## Governing Documents

`docs/dev/design/V7CallerOwnedStreamingAdoption.md`, `docs/dev/specifications/V7CallerOwnedStreamingAdoption.md`, and
`docs/dev/planning/done/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
