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
- Broader row-generator forms beyond the public row-expansion machinery already admitted in Sprint 25.

## Governing Plan

`docs/dev/planning/done/P07182601.V6-timeline-scan-recurrence.plan.md`

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
- Scan uses the relation-operation infrastructure delivered in Sprint 25, but its state-carrying contract, diagnostics,
  and acceptance fixtures remain separate from row-generation and set-composition features.

## Progress

- [x] (2026-07-27) Finalized the implementation-ready scan contract in
  `docs/dev/specifications/OrderedTimelineScan.md` and refreshed the governing plan for current plugin paths and
  Sprint 25 relation-operation capabilities.
- [x] (2026-07-27) Implemented the first code slice: public PySpark `scan(...)`, typed state callback capture,
  immutable scan operation/recipe records, ordinary-PySpark capability registration, and focused symbolic tests.
- [x] (2026-07-27) Implemented grouped-array online execution and generated-source rendering with public Spark
  `groupBy`, `collect_list`, `sort_array`, higher-order `aggregate`, `posexplode`, and `assert_true` guards.
- [x] (2026-07-27) Added focused Docker Compose live evidence for two-partition Fibonacci, empty input, transition
  reads from the current row, duplicate order-key failure, null order-key failure, and partition-bound failure:
  `pyspark35` and `pyspark40` each passed `tests/integration/pyspark/v6/test_ordered_timeline_scan.py` with 6 tests.
- [x] (2026-07-27) Added end-user scan documentation in `docs/QuickRef.md`, promoted the public catalog rows to
  implemented, and added completed user stories for ordered timeline recurrence.
- [x] (2026-07-27) Cleared unrelated PySpark 3.5 full-lane blockers found during release probing: passage-search
  projection fields, search generated-schema fixture coverage/order, long-valued generated similarity query labels,
  online higher-order lambda struct-field evaluation, and an integration-session AQE plan-string OOM.
- [x] (2026-07-27) Completed full release regression evidence. `make build` passed with 1310 tests passing, 39
  skipped, the supplemental rigidity pass passed with 34 tests passing and 6 skipped, and package build produced both
  sdist and wheel. Full Docker Compose integration passed for ordinary PySpark: `pyspark35` passed with 35 tests and 3
  skipped in 6:02; `pyspark40` passed with 38 tests in 6:54.
