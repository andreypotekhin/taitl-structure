# Sprint 54: V10 Hardening and Release Evidence

Status: completed with external evidence gaps; closeout: 2026-08-22; planned target: 2026-12-11. Environment-independent
hardening and evidence reconciliation are complete; live target lanes remain blocked by Docker-engine access and
SearchDocuments streaming remains design-gated.

## Sprint Goal

Verify and close V10 without admitting new feature scope.

Closeout: `make build` and the secondary rigidity/compatibility gate pass, package artifacts build successfully, and
the release evidence report names every retained gate and unavailable lane. V10 is conditionally closed pending the
external evidence listed in [V10 Release Evidence](../V10ReleaseEvidence.md).

## User-Facing Outcome

V10 support claims, generated artifacts, documentation, diagnostics, compatibility behavior, and build outputs are
reproducible and mutually consistent.

## Implementation Tasks

- Run focused no-Spark and all available PySpark 3.5/4.0 lanes.
- Run restart, generated-source, optional-provider, documentation, and performance checks.
- Fix only release blockers and record every retained gate and deferred owner.
- Implement and verify collision-safe generated identities across the Iterable plugin, PySpark schema symbols,
  generated documentation, plugin file-map merging, and generated-file writers.
- Run `make build` and publish the final evidence report.
- Run the scalar-generator/Search chunking freshness checks, focused regressions, `make gold`, and `make build` for
  `P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`.

## Acceptance and Demo

Every claimed support row has evidence; every gated row has a missing contract; generated artifacts are fresh; exact
pass/skip totals and unavailable lanes are recorded; and `make build` passes.

## Risks and Non-Goals

No new API scope. A skipped lane remains unavailable evidence rather than a release claim.

## Governing Plan

`docs/dev/planning/P08022604.V10-evidence-catalog-reconciliation-and-hardening.plan.md` and
`docs/dev/planning/P08042601.Collision-safe-generated-identities.plan.md` and
`docs/dev/planning/P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`.
