# Sprint 17: v.3 Incremental Compile and Cache Diagnostics

## Sprint Goal

Add production incremental compile after v.3's PySpark parity and streaming orchestration surfaces are stable.

## Product Outcome

Developers get fast feedback in large projects without recompiling unaffected transforms, and cache diagnostics explain
why each transform was reused or recompiled.

## Scope

### In Scope

- `compile --changed-only`.
- Cache invalidation rules for source, configuration, schema, dependency, generated-target, target-profile, and
  v.3 lifecycle-policy changes.
- Cache diagnostics for reused, recompiled, invalidated, and skipped transforms.
- Performance fixtures for synthetic 10-transform and 100-transform projects.
- No-Spark compiler command preservation.

### Out of Scope

- Changing transform semantics to make caching easier.
- Trusting generated-file hashes without verifying source and configuration fingerprints.
- Runtime Spark result caching.

## ExecPlan

`docs/dev/planning/P07092601.V3-incremental-compile-cache-diagnostics.plan.md`

## Engineering Tasks

1. Define stable cache keys and invalidation inputs.
2. Implement `compile --changed-only`.
3. Add cache diagnostics and explain/profile output.
4. Add performance fixtures and regression tests.
5. Update public CLI, testing, and troubleshooting docs.

## Acceptance Criteria

- No-change incremental compile avoids symbolic execution and regeneration for unchanged transforms.
- One-transform source changes recompile the changed transform and affected dependents only.
- Cache diagnostics explain reuse and invalidation decisions.
- `structure check`, `structure compile`, and `structure compile --fail-on-diff` remain Spark-free.
- `make build` passes.

## Progress

- [ ] Implement v.3 incremental compile and cache diagnostics.
