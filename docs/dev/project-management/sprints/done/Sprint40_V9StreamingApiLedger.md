# Sprint 40: V9 Streaming API Ledger

Status: complete. The checked PySpark streaming API ledger and guard tests are in place. Focused evidence:
`PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/compatibility/test_pyspark_streaming_api_coverage.py tests/specifications/compatibility/test_pyspark_structured_streaming_coverage.py tests/specifications/compatibility/test_pyspark_transformation_coverage.py`
passed with 11 tests on 2026-07-29.

## Sprint Goal

Create the checked PySpark Structured Streaming API ledger that governs v9 scope.

## User-Facing Outcome

A developer can inspect one resource and see whether a PySpark streaming API family is Structure-supported,
caller-owned-guided, design-gated, streaming-ineligible, or out of scope.

## In Scope

- `pyspark-streaming-api-coverage.json` under the PySpark plugin resources.
- Guard tests for status values, evidence paths, owner boundaries, and support-claim accounting.
- Classification of transformation APIs, DataStreamReader, DataStreamWriter, triggers, checkpoints, output modes,
  query lifecycle, foreach APIs, listeners, arbitrary state APIs, and Spark Connect streaming.
- Reconciliation of streaming-relevant v7 and v8 deferred items.

## Out of Scope

- New implementation support for a streaming API family.
- Structure-owned query lifecycle.
- Release hardening.

## Acceptance

The new ledger test passes and fails if a selected PySpark streaming API family is unclassified or if a lifecycle API
is counted as transformed-DataFrame support without an approved lifecycle-owning decision.

## Governing Documents

`docs/dev/specifications/V9PySparkStreamingApiCoverage.md` and
`docs/dev/planning/done/P07292602.V9-pyspark-streaming-api-coverage.plan.md`
