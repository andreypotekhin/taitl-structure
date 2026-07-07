# Sprint 13: v3 Aggregation PySpark Parity

## Sprint Goal

Close the planned aggregation gaps from `docs/dev/Gaps.md`: explicit grouping sets and post-aggregate
`having(...)`.

## Product Outcome

Developers can create custom subtotal layouts and filter aggregate result rows without leaving Structure's typed
aggregate DSL.

## Scope

### In Scope

- Explicit grouping sets.
- Aggregate-output predicate scope for `having(...)`.
- Diagnostics that distinguish pre-aggregate `where(...)`, metric-local filters, and post-aggregate `having(...)`.
- Backend capability checks, docs, compatibility tables, explain, traceability, generated examples, and parity tests.

### Out of Scope

- PySpark dict/list aggregate shorthand.
- Exact percentile family unless the gap status changes.
- Additional stats such as skewness, kurtosis, and mode unless the gap status changes.

## ExecPlan

`docs/dev/planning/P07072604.V3-aggregation-pyspark-parity.plan.md`

## Engineering Tasks

1. Implement grouping-set source capture, IR, validation, and lowering.
2. Implement `having(...)` source capture, aggregate-output binding, validation, and lowering.
3. Update docs, compatibility tables, generated examples, explain, traceability, and tests.

## Acceptance Criteria

- Grouping sets produce detail, subtotal, and grand total rows with correct nullable output fields.
- `having(...)` filters aggregate outputs and rejects unavailable pre-aggregate fields.
- Online and generated execution have parity coverage.
- `make build` passes.

## Progress

- [ ] Implement v3 aggregation parity.
