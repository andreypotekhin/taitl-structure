# Sprint 15: v.3 Higher-Order and Collection Helper PySpark Parity

## Sprint Goal

Close the planned collection helper gaps from `docs/dev/Gaps.md` without changing row cardinality.

## Product Outcome

Developers can express common nested-data operations for arrays and maps without hooks.

## Scope

### In Scope

- Collection size, array membership, and map-key membership helpers.
- Array construction, repeat, union, and except helpers.
- Element lookup, safe element lookup, and map concatenation helpers.
- Type unification, missing-key nullability, duplicate-key behavior, backend capability checks, diagnostics, docs,
  compatibility tables, explain, traceability, generated examples, and parity tests.

### Out of Scope

- Row-expanding generator helpers such as `explode(...)`, `posexplode(...)`, and `inline(...)`.
- Arbitrary Python control flow in higher-order callbacks.
- Unsupported callback lowering through UDFs.

## ExecPlan

`docs/dev/planning/P07072606.V3-collection-helper-pyspark-parity.plan.md`

## Engineering Tasks

1. Implement collection size, array membership, and map-key membership helpers.
2. Implement array construction, repeat, union, and except helpers.
3. Implement element lookup, safe element lookup, and map concatenation helpers.
4. Update docs, compatibility tables, generated examples, explain, traceability, and tests.

## Acceptance Criteria

- Planned collection helpers compile to readable PySpark and run online/generated with parity.
- Type conflicts, missing-key ambiguity, duplicate-key ambiguity, and unsupported generators produce diagnostics.
- `make build` passes.

## Progress

- [ ] Implement v.3 collection helper parity.
