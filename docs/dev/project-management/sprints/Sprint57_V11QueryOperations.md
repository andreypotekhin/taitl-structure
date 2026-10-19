# Sprint 57: V11 Relational Query Operations

Status: implementation, ordinary/Connect PySpark 4.1.0 parity, and `make build` complete; target: 2027-01-22.

## Sprint goal

Add the approved PySpark 4.1 existence/IN-subquery and typed lateral relation slice without hiding DataFrame callbacks.

## User-facing outcome

Users can express admitted correlated predicates and relation operations with explicit aliases, cardinality, null
behavior, and explainable dependencies.

## Implementation tasks

- [x] Extend immutable relation-query IR, scope validation, cardinality metadata, evaluator, renderer, explain, and traceability.
- [x] Add duplicate, nullable input, correlation, ordered-limit, post-limit predicate, and lateral cardinality fixtures.
- [x] Run focused ordinary and Connect 4.1 native/online/generated parity.
- [x] Complete `make build` and close this sprint record.

## Acceptance

Online/generated results and schemas match; invalid capture and cardinality cases fail before lowering; raw DataFrame
callbacks remain caller-owned.

## Governing plan

[V11 query operations ExecPlan](../../planning/past/P10182601.V11-query-operations.plan.md) records the agreed
`where()`/`project()` query syntax, inferred and prepared lateral joins, optional `on=`, SQL null behavior,
explicit outer references, ordered limits, Cartesian opt-in, implementation sequence, and parity evidence.

The parent is the [V11 adoption ExecPlan](../../planning/P08042601.V11-pyspark-4.1-adoption.plan.md).
The [query design](../../design/V11PySpark41QueryOperations.design.md) and parity specification now describe the
implemented contract. Both ordinary and Connect `>=4.1,<4.2` query capabilities are admitted; the default profile
remains unchanged.

## Evidence

On 2026-10-18, `tests/integration/pyspark/v11/test_query_operations.py` passed both cases on ordinary PySpark/Spark
4.1.0 and Spark Connect/PySpark/Spark 4.1.0. The rerun includes empty IN-subquery behavior. Each case compared online
Structure and generated PySpark with native PySpark values and schemas. Coverage includes correlated EXISTS, scalar
and composite IN with nullable inputs, generated-source scans, and a left lateral query with ordered limits, duplicate
matches, a post-limit `on=`, and a missing-match row. The direct query specification and user-story selection passed
43 tests; the broader Spark-free selection passed 147 tests. The complete build and the unrelated earlier-profile
Delta CDF limitation are recorded in the archived ExecPlan.
