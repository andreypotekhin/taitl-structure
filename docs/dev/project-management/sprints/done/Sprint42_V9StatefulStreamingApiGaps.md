# Sprint 42: V9 Stateful Streaming API Gaps

Status: complete. The v9 ledger now records operation-level streaming decisions for distinct-style relation sets,
ordering/bounds, priority selection, selected-row helpers, and analytic window projections. Focused evidence:
`PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/compatibility/test_pyspark_streaming_api_coverage.py tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py`
passed with 58 tests on 2026-07-29. No new stateful streaming shape was admitted.

## Sprint Goal

Re-evaluate stateful and order-sensitive streaming API gaps from v7 and v8 under current PySpark 3.5/4.0 behavior.

## User-Facing Outcome

Stateful streaming shapes either work with explicit watermark, bound, composition, and output-mode guidance, or fail
before query start with a precise diagnostic.

## In Scope

- Design gates for selected-row helpers, analytic ranking windows, lag/lead, rolling windows, distinct-style set
  operations, arbitrary ordering, limits, offsets, and priority selection.
- Revalidation of one-stateful-plus-stateless composition diagnostics.
- Live restart evidence for any admitted shape.
- Ledger updates for every accepted or rejected candidate.

## Out of Scope

- Unbounded state.
- Two-stateful chains unless Spark evidence and compiler-visible state rules prove a safe narrow contract.
- Lifecycle APIs, sinks, triggers, checkpoints, and side effects.

## Acceptance

No v7 or v8 streaming-relevant deferred item remains ambiguous. Every admitted shape has PySpark 3.5/4.0 evidence, and
every rejected shape has a corrective diagnostic and ledger evidence.

## Governing Documents

`docs/dev/specifications/V9PySparkStreamingApiCoverage.md`,
`docs/dev/specifications/V7CallerOwnedStreamingAdoption.md`,
`docs/dev/specifications/V8StructuredStreamingCoverageParity.md`, and
`docs/dev/planning/done/P07292602.V9-pyspark-streaming-api-coverage.plan.md`
