# Sprint 41: V9 Caller-Owned Adoption Recipes

Status: complete. `examples/streams/adoption.py` now contains the explicit caller-owned lifecycle recipe used by the
live file-stream integration fixture. Local evidence:
`PYTHONPATH=.:src:tests poetry run pytest -q tests/differential/streams/test_streams_reference_contract.py tests/specifications/compatibility/test_pyspark_streaming_api_coverage.py`
passed with 8 tests on 2026-07-29. Live PySpark 3.5 and 4.0 targeted lanes each passed with 2 tests on 2026-07-29.

## Sprint Goal

Make the caller-owned source, sink, checkpoint, trigger, output-mode, and query lifecycle boundary easy to apply in real
PySpark Structured Streaming code.

## User-Facing Outcome

A developer can copy a tested recipe that creates a streaming source, runs a Structure transform online or through
generated PySpark, writes to a caller-owned sink, and restarts from a caller-owned checkpoint.

## In Scope

- Runnable examples under `examples/streams`.
- Generated artifacts for the streaming examples.
- File-stream restart tests for online and generated execution.
- Output-mode guidance for admitted stateful operations.
- Generated-source scans proving transform modules still contain no lifecycle or action calls.

## Out of Scope

- Structure-generated sources or sinks.
- Query deployment, monitoring, alerting, or recovery orchestration.
- `foreachBatch` side-effect ownership.

## Acceptance

The example runs with caller-owned lifecycle code, restart evidence passes in the live PySpark lanes, and docs clearly
show which lines are ordinary PySpark code and which lines are Structure transform execution.

## Governing Documents

`docs/dev/specifications/V9PySparkStreamingApiCoverage.md` and
`docs/dev/planning/done/P07292602.V9-pyspark-streaming-api-coverage.plan.md`
