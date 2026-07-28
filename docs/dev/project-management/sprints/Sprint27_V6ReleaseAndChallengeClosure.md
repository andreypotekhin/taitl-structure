# Sprint 27: V6 Release Evidence and Challenge Closure

## Sprint Goal

Turn completed v6 capability work into a trustworthy release: prove its behavior across supported PySpark targets,
finish its documentation, and leave no unowned challenge or raw-hook claim.

## Product Outcome

Users can determine which PySpark patterns belong in a Structure step, which deliberately stay raw, and how to adopt
the release. Maintainers have explicit evidence rather than inferred completion.

## Scope

### In Scope

- Full build, generated-artifact, API-ledger, AST-boundary, online/generated parity, and PySpark 3.5/4.0 live evidence.
- C27--C34 audit and documented resolution, named deferral, or project-owner escalation.
- C28 operational recipes and troubleshooting links; C30 executable-specification matrix; C31 decision record and
  publication checklist; C32 alias audit; C33 composed-hook ownership design; C34 ledger-backed hook inventory.
- Public reference, capability, diagnostics, performance/memory guidance, and release-note updates.

### Out of Scope

- New feature families or speculative API additions.
- Resolving licensing/governance policy without a project-owner decision.
- Treating skipped Spark tests as passing target evidence.

## Governing Plan

`docs/dev/planning/P07242604.V6-pyspark-api-and-example-hook-retirement.plan.md`

## Acceptance Criteria

- `make build` and each required live target lane pass with exact results recorded.
- The ledger and raw-hook inventory agree with docs, examples, capabilities, and tests.
- C27--C34 each link to a resolution, a separately scheduled plan, or an explicit owner decision; no stale “remaining”
  implementation claim remains.
- Adoption recipes can be followed in a clean checkout and troubleshooting guidance names practical recovery actions.
- The project has no unreviewed change to public imports, generated output, or supported-target policy.

## Risks and Controls

- Release evidence can be mistaken for local unit success: distinguish Spark-free results, live passes, and skipped
  lanes in every summary.
- C31 needs authority outside implementation: prepare the decision record but mark publication blocked until the owner
  chooses the license/governance model.

## Progress

- [x] Run and record release evidence.
- [x] Complete Challenge C27--C34 disposition.
- [x] Publish adoption and troubleshooting documentation.
- [ ] Prepare v6 release handoff.

## Evidence

- 2026-07-28 local `make build`: lint passed, mypy passed, pytest passed with 1,313 passed and 40 skipped, rigidity
  subset passed with 34 passed and 6 skipped, and source/wheel distributions built. Live PySpark lanes were skipped in
  this workspace and are not claimed as live target passes.
