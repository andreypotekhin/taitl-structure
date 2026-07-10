# Sprint 14: v.3 Window PySpark Parity

## Sprint Goal

Close the planned window gaps from `docs/dev/Gaps.md`: null ordering, multiple order keys everywhere, and aggregate
windows.

## Product Outcome

Developers can define practical windowed analytics with explicit ordering, deterministic null placement, and aggregate
metrics over reusable window specs.

## Scope

### In Scope

- Null ordering in window order keys.
- Multiple order keys in all window helpers.
- Aggregate windows mirroring admitted aggregate helpers.
- Backend capability checks, diagnostics, docs, compatibility tables, explain, traceability, generated examples, and
  parity tests.

### Out of Scope

- Raw PySpark `WindowSpec` escape hatches.
- Window helpers for aggregate functions not otherwise admitted by Structure.
- Streaming support for stateful windows unless the streaming orchestration plan explicitly admits it.

## ExecPlan

`docs/dev/planning/P07072605.V3-window-pyspark-parity.plan.md`

## Engineering Tasks

1. Add order descriptors for nulls first and nulls last.
2. Normalize multi-key ordering across window helpers.
3. Implement aggregate window helpers.
4. Update docs, compatibility tables, generated examples, explain, traceability, and tests.

## Acceptance Criteria

- Window helpers render multiple ordered keys with explicit null ordering.
- Aggregate windows run online/generated with parity.
- Invalid frames and unsupported helper combinations fail before runtime.
- `make build` passes.

## Progress

- [ ] Implement v.3 window parity.
