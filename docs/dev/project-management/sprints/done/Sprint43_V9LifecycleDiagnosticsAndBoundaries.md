# Sprint 43: V9 Lifecycle Diagnostics and Boundaries

Status: complete. Streaming hook warnings, public diagnostics, streaming API docs, and troubleshooting now separate
Structure-owned transformations from caller-owned PySpark lifecycle and side-effect code. Focused evidence:
`PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py tests/differential/streams/test_streams_reference_contract.py`
passed with 56 tests on 2026-07-29.

## Sprint Goal

Make streaming lifecycle and side-effect boundaries explicit in diagnostics, explain output, and documentation.

## User-Facing Outcome

When a streaming transform or example touches sources, sinks, output modes, checkpoints, triggers, query lifecycle, or
side effects, Structure tells the developer whether the API is caller-owned, design-gated, or unsupported, and what to
do next.

## In Scope

- Diagnostic wording and documentation links for DataStreamReader, DataStreamWriter, triggers, checkpoints, output
  modes, query lifecycle, `foreach`, `foreachBatch`, listeners, RDD/Pandas conversion, actions, and hidden UDF fallback.
- Explain output that separates Structure-owned transformation state from caller-owned lifecycle requirements.
- Troubleshooting entries for common streaming adoption failures.
- Ledger evidence updates.

## Out of Scope

- Owning query lifecycle.
- Executing side-effecting sinks inside generated transform modules.
- Spark Connect streaming support unless the ledger classifies it as unclaimed.

## Acceptance

Focused diagnostics tests pass, explain output names owner boundaries for streaming-sensitive APIs, and public
troubleshooting docs tell users how to fix or place each API safely.

## Governing Documents

`docs/dev/specifications/V9PySparkStreamingApiCoverage.md`,
`docs/dev/specifications/SparkStreamingDeferredFeatures.md`, and
`docs/dev/planning/done/P07292602.V9-pyspark-streaming-api-coverage.plan.md`
