# Sprint 37: V8 Stateless Streaming Gaps

## Sprint Goal

Admit the safe stateless portions of the current batch-only streaming gaps.

## User-Facing Outcome

Typed row-local expansion and union-like transformations that Spark accepts on streaming DataFrames work in online and
generated Structure execution without Structure owning query lifecycle.

## In Scope

- Design and implementation for streaming-safe typed struct generators if live PySpark accepts them.
- Operation-level set-family split, with union-like operations admitted only when restart evidence passes.
- Static diagnostics for set operations that Spark rejects on streaming DataFrames.
- Explain, traceability, generated-source scans, and PySpark 3.5/4.0 file-stream restart evidence.

## Out of Scope

- Distinct, intersect, subtract, and except streaming claims without explicit Spark evidence.
- Ordering, limits, selected-row priority selection, lifecycle ownership, and Spark Connect streaming.

## Acceptance

Each admitted stateless operation returns a streaming DataFrame in online and generated execution, survives restart from
a caller-owned checkpoint, and keeps generated source free of lifecycle and action APIs.

## Governing Documents

`docs/dev/specifications/V8StructuredStreamingCoverageParity.md` and
`docs/dev/planning/P07292601.V8-pyspark-structured-streaming-coverage-parity.plan.md`
