# Sprint 16: v.3 Streaming Orchestration

## Sprint Goal

Move beyond caller-owned streaming DataFrames by adding Structure-owned streaming source, sink, and lifecycle policy
contracts.

## Product Outcome

Developers can declare a streaming job in Structure, generate reviewable `readStream` and `writeStream` PySpark, and
configure triggers, checkpoints, output modes, watermarks, and admitted state policies.

## Scope

### In Scope

- Streaming source declarations.
- Streaming sink declarations.
- Generated `readStream`.
- Generated `writeStream`.
- Trigger configuration.
- Checkpoint configuration.
- Output mode configuration.
- Watermarks.
- Admitted state policies.
- Lifecycle diagnostics, docs, compatibility tables, explain, generated examples, and live streaming evidence.

### Out of Scope

- Custom side-effect sinks such as `foreachBatch` and `foreach`.
- Arbitrary hook-managed streaming lifecycle.
- Broad stream-stream joins unless bounded, watermarked semantics are specified in the ExecPlan first.
- Hidden streaming lifecycle behavior in ordinary batch transforms.

## ExecPlan

`docs/dev/planning/P07072607.V3-streaming-orchestration.plan.md`

## Engineering Tasks

1. Add lifecycle declaration model.
2. Add generated source and sink rendering.
3. Add trigger, checkpoint, output mode, watermark, and state policy validation.
4. Add online lifecycle runner or query-builder decision and implementation.
5. Add integration evidence and public docs.

## Acceptance Criteria

- A minimal declared streaming job generates reviewable `readStream` and `writeStream` PySpark.
- Missing checkpoint, invalid output mode, missing watermark for stateful behavior, and unsupported custom sinks fail
  with diagnostics.
- Existing caller-owned streaming compatibility behavior remains valid.
- Default `make build` passes, and opt-in streaming integration evidence is recorded.

## Progress

- [ ] Implement v.3 streaming orchestration.
