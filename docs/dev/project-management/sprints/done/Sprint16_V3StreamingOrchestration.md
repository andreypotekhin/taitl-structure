# Sprint 16: v3 Streaming Transformation Hardening

## Sprint Goal

Make admitted streaming transformations demonstrably usable while preserving caller ownership of Spark source, sink,
and lifecycle policy.

## Product Outcome

Developers can use generated and online transforms over caller-created streaming DataFrames, see state and output-mode
requirements in explain output, and validate the same behavior with live file-stream evidence.

## Scope

### In Scope

- Watermarked enrichment, dedupe, aggregation, and bounded stream-stream correlation evidence.
- Caller-required output-mode and state-policy diagnostics.
- Generated examples, documentation, compatibility tables, and live file-stream evidence.

### Out of Scope

- All source, sink, trigger, checkpoint, query, deployment, and recovery ownership.
- Custom side-effect sinks such as `foreachBatch` and `foreach`.
- Arbitrary hook-managed streaming lifecycle.
- Broad stream-stream joins unless bounded, watermarked semantics are specified first.

## ExecPlan

`docs/dev/planning/done/P07122601.Streams-example-and-caller-owned-streaming.plan.md`

## Engineering Tasks

1. Add the streams example and generated contract.
2. Add caller-owned file-stream integration evidence.
3. Update streaming diagnostics, docs, compatibility tables, and project plans.

## Acceptance Criteria

- The streams example demonstrates every admitted transformation shape without generating lifecycle code.
- Missing watermarks and invalid stream-stream bounds fail with diagnostics; required output modes are reported.
- Existing caller-owned streaming compatibility behavior remains valid.
- Default `make build` passes, and opt-in streaming integration evidence is recorded.

## Progress

- [x] (2026-07-12) Implemented and verified streaming transformation hardening.
