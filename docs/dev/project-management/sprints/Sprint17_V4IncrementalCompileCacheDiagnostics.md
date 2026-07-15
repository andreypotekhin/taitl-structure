# Future Backlog: Incremental Compile and Cache Diagnostics

## Future Goal

Add production incremental compile after the v4 transformation coverage program has established the supported
transformation surface and its capability shapes.

## Product Outcome

Developers get fast feedback in large projects without recompiling unaffected transforms, and cache diagnostics explain
why each transform was reused or recompiled.

## Placement

This remains a valid future work package. It is not Sprint 17, has no assigned version, and does not set the v4 release
direction. See `Sprint17_V4TransformationApiCoverage.md` for the opening v4 work.

## Scope

### In Scope

- `compile --changed-only`.
- Cache invalidation rules for source, configuration, schema, dependency, generated-target, target-profile, and
  completed v3 streaming-policy changes.
- Cache diagnostics for reused, recompiled, invalidated, and skipped transforms.
- Performance fixtures for synthetic 10-transform and 100-transform projects.
- No-Spark compiler command preservation.

### Out of Scope

- Changing transform semantics to make caching easier.
- Trusting generated-file hashes without verifying source and configuration fingerprints.
- Runtime Spark result caching.

## ExecPlan

`docs/dev/planning/P07092601.V4-incremental-compile-cache-diagnostics.plan.md` (future work; filename retained for
history)

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

- [ ] Implement future incremental compile and cache diagnostics when reprioritized.
