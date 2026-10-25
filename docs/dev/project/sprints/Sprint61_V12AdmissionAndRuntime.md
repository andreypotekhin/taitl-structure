# Sprint 61: V12 Admission and Runtime Foundation

Status: active; preparation complete 2026-10-25, runtime implementation pending. Iteration 1 of 12; two weeks,
with calendar dates unset.

## Sprint goal

Make PySpark 4.2 a reproducible target and account for every inherited V11 capability.

## User-facing outcome

Users can select the exact 4.2 target and distinguish retained behavior from a feature-specific admission gap.

## Dependencies

Start from the V11 release evidence. This sprint supplies the runtime foundation for Sprints 62–71.

## Implementation tasks

- [ ] Pin ordinary and Connect 4.2 client/engine environments and add the planned pyspark42/spark-connect42 runner
      selectors.
- [ ] Inventory the 4.1-to-4.2 Python API and semantic delta, including SQL-only additions and caller-owned APIs.
- [ ] Map every V11 capability to a 4.2 carry-forward test or a named feature-sprint issue; do not infer profile
      inheritance.
- [ ] Exercise Arrow defaults, PyArrow >=18, client/engine version guards, and delayed Connect column resolution.
- [ ] Probe Delta, Iceberg, Sedona, and managed-session availability; record exact compatible packages or missing
      artifacts.
- [x] Record the V11 retrospective and classify the runtime-check full-rerun follow-up without replacing dated release
      evidence. All five pending lanes passed unfiltered on 2026-10-25; results are in the archived runtime-check plan.
- [ ] Assign every reviewed row a sprint, status, variant, streaming classification, remedy, and evidence location.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [V11 parity](../../specifications/V11PySpark41Parity.spec.md).
- [Backend capabilities](../../specifications/BackendCapabilities.spec.md).
- [Compatibility policy](../../specifications/CompatibilityPolicy.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- The full configured integration matrix passes at sprint close, with evidence recorded under the
  [validation cadence](../../Testing.md#validation-cadence).
- Both new lanes start and report pinned versions; retained six-lane selections remain runnable with unchanged coverage.
- The 4.2 inventory and V11 continuity map have no unassigned rows; differences are either repaired or assigned to a
  package.
- Provider availability and eight-lane baseline selection counts are recorded; unavailable evidence is not called
  passing.

## Demo and validation

Run a V11 projection/query fixture natively, through Structure, and as generated code on both new lanes; compare schema
and normalized rows. Show an unsupported-profile diagnostic and a client/engine mismatch diagnostic.

During coding tasks, run `make build` and choose focused live checks as needed. At sprint close, run the full
configured matrix against the closing candidate, including required isolated provider phases:

```bash
make build INTEGRATION=1 BACKEND=all
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

Profile-list duplication can silently omit capabilities. Compare the inventory with capabilities, diagnostics,
runner selection, and generated-runtime checks. Resolve provider feasibility early.

## Non-goals

No default-profile promotion, blanket feature admission, Connect streaming, or implementation of later feature packages.

## Governing documents

[Sprint 61 implementation plan](../../planning/P10252601.V12-admission-and-runtime.plan.md).

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
