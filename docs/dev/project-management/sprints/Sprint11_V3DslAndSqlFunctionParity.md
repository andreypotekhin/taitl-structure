# Sprint 11: v3 DSL and SQL Function PySpark Parity

## Sprint Goal

Close the planned `DSL` gaps from `docs/dev/Gaps.md` so common scalar Column operations and SQL functions remain
compiler-visible.

## Product Outcome

Developers can write ordinary predicates, casts, ordering, string operations, date/time helpers, numeric helpers, and
predicate helper functions in Structure source without dropping into hooks.

## Scope

### In Scope

- Membership predicates.
- Range predicates.
- String predicates.
- Collection indexing and struct field helpers.
- Rich casts.
- Ordering modifiers including null ordering descriptors needed by later window work.
- Broader string SQL helpers.
- Date/time helpers.
- Numeric/math helpers.
- Predicate helper functions.
- Backend capability checks, diagnostics, explain, traceability, docs, compatibility tables, and parity tests.

### Out of Scope

- Raw SQL string expressions.
- Raw PySpark Column aliases.
- Raw `Column.over(...)`.
- Bitwise methods, struct mutation, and null/NaN expansion unless the gap status changes.
- UDF/UDTF helper admission.

## ExecPlan

`docs/dev/planning/P07072602.V3-dsl-and-sql-function-pyspark-parity.plan.md`

## Engineering Tasks

1. Add source helper APIs and symbolic expression records.
2. Add type inference and nullability rules.
3. Add shared PySpark lowering for online and generated execution.
4. Add backend capability diagnostics for target-specific differences.
5. Add docs, compatibility rows, generated examples, and parity tests.

## Acceptance Criteria

- Planned Column API gaps compile to readable PySpark Column operations.
- Planned SQL function gaps compile to readable `pyspark.sql.functions` calls.
- Unsupported raw and opaque expression forms continue to fail before runtime with diagnostic links.
- `make build` passes.

## Progress

- [ ] Implement v3 DSL and SQL function parity.
