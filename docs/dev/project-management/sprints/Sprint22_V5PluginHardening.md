# Sprint 22: V5 Plugin Plugin Architecture Hardening

## Sprint Goal

Close the breaking migration, remove obsolete target coupling, and prove v5 ready for release.

## Product Outcome

Users and plugin authors have one coherent plugin model, PySpark behavior remains trustworthy, and no compatibility
shim obscures which package owns target APIs.

## Scope

### In Scope

- Immediate removal of target-owned `structure` root exports and migration of examples and fixtures.
- Removal of legacy backend configuration, compatibility reporting, and PySpark-specific Core dispatch.
- Upgrade, extension, capability, diagnostics, troubleshooting, and release documentation.
- Full regression, conformance, generated-artifact, supported-target, and performance-baseline evidence.
- Resolution or explicit deferral of v5 release blockers.

### Out of Scope

- New target plugins or transformation feature families.
- Cross-plugin pipelines, translation, or data interchange.
- Public support for private engine replacement.

## ExecPlan

`docs/dev/planning/P07162601.V5-plugin-architecture.plan.md`

## Acceptance Criteria

- No target-owned authoring name remains exported from the package root.
- No Core workflow imports a concrete PySpark plugin façade or service-facet implementation.
- PySpark and external-plugin conformance evidence pass against the released Plugin API range.
- Documentation consistently describes one target per transform and Core-owned workflows.
- Private replacement engines remain target-local, exact-revision-gated, and absent from public authoring guidance.
- `make build` and every required live target lane pass.

## Progress

- [ ] Start after Sprint 21 closes.
