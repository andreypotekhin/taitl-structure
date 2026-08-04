# Sprint 57: V11 Relational Query Operations

Status: planned; target: 2027-01-22.

## Sprint goal

Add the approved PySpark 4.1 existence/IN-subquery and typed lateral relation slice without hiding DataFrame callbacks.

## User-facing outcome

Users can express admitted correlated predicates and relation operations with explicit aliases, cardinality, null
behavior, and explainable dependencies.

## Implementation tasks

- Extend relation IR, scope validation, cardinality metadata, evaluator, renderer, explain, and traceability.
- Add duplicate, empty, null, correlation, alias, and lateral cardinality fixtures.
- Run ordinary and Connect 4.1 evidence where upstream support is documented.

## Acceptance

Online/generated results and schemas match; invalid capture and cardinality cases fail before lowering; raw DataFrame
callbacks remain caller-owned.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md` and `docs/dev/design/V11PySpark41QueryOperations.md`.
