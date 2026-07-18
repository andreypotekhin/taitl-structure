# Final v4 Hardening Sprint

## Sprint Goal

Make v4 ready to release after its final feature sprint by resolving release-blocking defects and proving the released
transformation API coverage is stable, documented, and compatible with its supported targets.

## Product Outcome

Developers can upgrade to v4 with trustworthy transformation coverage, actionable diagnostics, current documentation,
and evidence that online execution and generated PySpark remain equivalent across the supported target range.

## Schedule

This is the terminal sprint of v4. It starts only after every scheduled v4 feature sprint is complete and before the
v4 release is cut.

## Scope

### In Scope

- Resolve v4 release-blocking regressions and diagnostic, documentation, or generated-artifact defects.
- Run the full regression suite, online/generated parity coverage, supported-target compatibility checks, and
  generated-artifact freshness checks.
- Run each admitted feature family's live concept-parity scenario through `pyspark35` and `pyspark40`; run Spark
  Connect lanes only for capability profiles that claim Connect support.
- Recheck the transformation coverage catalog for complete, accurate classification and evidence links.
- Verify release documentation, upgrade guidance, compatibility claims, troubleshooting links, and release notes.
- Capture performance baselines for affected compiler paths and investigate material regressions.
- Record non-blocking discoveries as separately scheduled follow-up work.

### Out of Scope

- New transformation APIs, broad helper families, or expansion of the v4 release boundary.
- New backends, loading, storage, actions, orchestration, or non-batch Spark Connect work.
- Refactoring that is not necessary to resolve a release blocker.

## Acceptance Criteria

- `make build` passes.
- The v4 coverage catalog has one accurate classification and appropriate evidence for every in-scope API.
- Online and generated PySpark behavior has parity evidence for every v4-supported API family.
- Supported-target compatibility, generated-artifact freshness, diagnostics, public documentation, and troubleshooting
  references are verified for release.
- The hardening ExecPlan records the exact default-build and Compose commands, backend versions, passed/skipped totals,
  and explicit deferrals. A skipped live lane does not count as release evidence.
- All remaining defects are either resolved or explicitly deferred with a documented rationale and follow-up item.

## Progress

- [x] Started 2026-07-17 after Sprint 18.
- [x] Completed 2026-07-17: catalog/golden verification, default build (1081 passed, 22 skipped), and successful
  `pyspark35`/`pyspark40` Compose lanes. Public V4 documentation now reflects delivered streaming migration forms and
  explicit deferrals; benchmark baselines remain non-release-blocking until benchmark infrastructure exists.
