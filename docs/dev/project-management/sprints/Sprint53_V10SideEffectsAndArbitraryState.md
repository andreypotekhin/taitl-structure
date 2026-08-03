# Sprint 53: V10 Side-Effect Safety and Arbitrary State

Status: planned; target: 2026-11-27.

## Sprint Goal

Publish caller-owned side-effect safety guidance and an implementation-ready arbitrary-state model.

## User-Facing Outcome

Streaming applications can place Structure transforms inside explicit, idempotent sink workflows without mistaking
caller-owned lifecycle code for Structure support.

## Implementation Tasks

- Document sink identity, idempotence, retry, checkpoint, failure, recovery, and security requirements.
- Test `foreachBatch` adoption and generated-source cleanliness.
- Define typed arbitrary-state input/state/output, timeout, initialization, cleanup, target, hook, and restart rules.
- Keep unsupported state APIs design-gated until implementation and live evidence exist.

## Acceptance and Demo

Adoption examples run outside generated modules; missing safety declarations produce useful diagnostics; no generated
module owns a sink or lifecycle call.

## Risks and Non-Goals

No Structure-owned job, sink, `foreach`, `foreachBatch`, checkpoint, or recovery runtime.

## Governing Plan

`docs/dev/planning/P08022603.V10-streaming-side-effects-and-arbitrary-state.plan.md`.
