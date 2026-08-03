# Sprint 52: V10 Streaming State Stages and Join Contracts

Status: planned; target: 2026-11-13.

## Sprint Goal

Make state composition and additional stream-stream join assumptions compiler-visible and evidence-based.

## User-Facing Outcome

Explain output and diagnostics identify state stages, watermarks, retention, event-time bounds, and caller-required
output modes before query start.

## Implementation Tasks

- Add ordered state-stage metadata to IR/compatibility analysis.
- Prototype bounded cross and anti stream-stream candidates with public Spark APIs.
- Preserve finite selected-value alternatives and broad batch-only analytic boundaries.
- Run PySpark 3.5/4.0 parity and restart lanes.

## Acceptance and Demo

Approved shapes pass symbolic/generated/online/restart evidence; unsafe compositions fail with named reasons.

## Risks and Non-Goals

No arbitrary second-stateful composition, global selected-row support, or lifecycle generation.

## Governing Plan

`docs/dev/planning/P08022602.V10-streaming-state-and-join-contracts.plan.md`.
