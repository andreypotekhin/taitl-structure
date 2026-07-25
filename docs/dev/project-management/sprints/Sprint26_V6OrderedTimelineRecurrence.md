# Sprint 26: V6 Ordered Timeline Recurrence

## Sprint Goal

Deliver a safe, bounded recurrence over caller-supplied PySpark timelines without pretending that an analytic window
or raw hook provides stateful feedback semantics.

## Product Outcome

A developer can express a typed Fibonacci-style recurrence with `scan(...)`, inspect its bound and ordering through
explain/traceability, and obtain equivalent online and generated batch PySpark output.

## Scope

### In Scope

- The public PySpark-plugin `scan(...)` contract from the dedicated recurrence ExecPlan.
- Typed initial state, state-transition callback capture, immutable scan records, and batch-only capability checks.
- Explicit partition/order keys, duplicate-key failure, positive per-partition maximum, and state-before-transition
  output semantics.
- Optimizer-visible grouped-array lowering through public DataFrame/Column APIs.
- Two-partition Fibonacci online/generated/live evidence and operational memory guidance.

### Out of Scope

- Global, unbounded, persistent, input-less, or streaming scans.
- UDF, RDD, Pandas, driver-loop, or raw-hook recurrence implementation.
- General row generators; those are owned by Sprint 25.

## Governing Plan

`docs/dev/planning/P07182601.V6-timeline-scan-recurrence.plan.md`

## Acceptance Criteria

- A finite caller timeline yields one declared row per input row and resets state for each partition.
- Duplicate order keys, null keys, invalid callback/state shape, over-bound partitions, streams, and unsupported
  placement fail through documented diagnostics.
- Generated source visibly orders, folds, and expands public PySpark expressions without hidden Python execution.
- Online/generated rows and schemas match for supported PySpark versions.
- `make build` passes.

## Risks and Controls

- Per-partition materialization can consume too much memory: enforce literal `max_rows`, document the bound, and do
  not claim unbounded/streaming support.
- Relation-operation work can overlap Sprint 25: share extracted recipe infrastructure, but keep scan's state contract
  and acceptance fixtures separate.

## Progress

- [ ] Finalize public scan contract and diagnostics.
- [ ] Implement capture, recipe, lowering, evaluation, and rendering.
- [ ] Add Fibonacci/live evidence and documentation.
- [ ] Run full release regression evidence.
