# Sprint 34: V7 Stream-Static Outer Lookup

Status: complete. Static compiler checks pin left-outer nullable lookup projection and fail-closed reverse or broad
stream-static join directions. Dedicated online and generated restart evidence passes on PySpark 3.5 and 4.0.

## Sprint Goal

Extend caller-owned enrichment with left-outer static lookup semantics and declared nullable lookup fields.

## In Scope

- Left-outer lookup schema/nullability rules, static diagnostics, explain, parity, generated-source scans, and file-stream restart evidence on PySpark 3.5/4.0.

## Out of Scope

- Any reverse outer direction, stateful composition, and lifecycle ownership.

## Acceptance

- Unmatched streaming rows are preserved and receive only the declared nullable lookup fields.
- Reverse or broad stream-static joins remain batch-only with corrective diagnostics.

## Progress

- [x] Add Spark-free static compatibility checks for left-outer stream-static lookup.
- [x] Add nullable lookup-field diagnostics for left-outer projection.
- [x] Add a dedicated online/generated restart fixture for left-outer lookup.
- [x] Run live PySpark 3.5 and 4.0 evidence for `tests/integration/pyspark/v7/test_stream_static_restart.py`.

## Evidence

- Spark-free focused checks passed with 42 tests:
  `PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py --maxfail=3`.
- Local live-test skip evidence passed with 2 expected skips:
  `PYTHONPATH=.:src:tests poetry run pytest -q tests/integration/pyspark/v7/test_stream_static_restart.py -rs`.
- Live PySpark 3.5 evidence passed with 5 tests and 3 skips:
  `docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm -e INTEGRATION_PYTEST_ARGS='/workspace/tests/integration/pyspark/v7/test_stream_static_restart.py' structure-integration-pyspark35`.
- Live PySpark 4.0 evidence passed with 8 tests using the same targeted fixture command against
  `structure-integration-pyspark40`.

## Governing Documents

`docs/dev/design/V7CallerOwnedStreamingAdoption.md`, `docs/dev/specifications/V7CallerOwnedStreamingAdoption.md`, and
`docs/dev/planning/done/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
