# Sprint 72: V12 Hardening and Closeout

Status: planned. Iteration 12 of 12; two weeks, with calendar dates unset.

## Sprint goal

Verify and close the integrated V12 release without adding feature scope.

## User-facing outcome

Users receive a reproducible 4.2 support claim, migration guidance, and matching documentation and runtime evidence.

## Dependencies

Follows all feature sprints. Unfinished core commitments require a recorded scope decision before release closure.

## Implementation tasks

- [ ] Resolve release-blocking defects and rerun affected native/online/generated and ordinary-streaming evidence.
- [ ] Run all eight documented unfiltered backend selections and isolated provider phases; retain selection counts and
      skip reasons.
- [ ] Run `make build INTEGRATION=1 BACKEND=all` at sprint close; compare generated freshness and compiler/plan
      performance baselines.
- [ ] Reconcile APIs, capabilities, diagnostics, references, examples, user-story tests, and relevant annotated source.
- [ ] Evaluate the default profile only after the passing matrix; record promotion or retention and exact-feature
      gating.
- [ ] Record migrations, troubleshooting, unresolved external provider gates, and the release retrospective.
- [ ] Mark M19 and completed user stories only after acceptance; archive completed ExecPlans and retain current-version
      sprints.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [Compatibility policy](../../specifications/CompatibilityPolicy.spec.md).
- [Backend capabilities](../../specifications/BackendCapabilities.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- Every core feature meets its acceptance or has an explicit scope revision; supported claims have positive evidence.
- Eight documented lane selections and the integration-enabled build pass; scoped selections are described accurately.
- Evidence and failure recovery follow the [validation cadence](../../Testing.md#validation-cadence); required
  release checks remain additional to the full sprint matrix.
- The profile decision, migration notes, generated artifacts, performance comparisons, and retrospective are recorded.
- Connect streaming and unavailable provider combinations remain excluded from support and release-pass accounting.

## Demo and validation

Run the eight-lane integrated release command, then demonstrate one batch query/ranking pipeline and one ordinary
streaming pipeline with restart. Review the exact runtime ledger and support/default-profile decision.

During coding tasks, run `make build` and choose focused live checks as needed. At sprint close, run the full
configured matrix against the closing candidate, including required isolated provider phases:

```bash
make build INTEGRATION=1 BACKEND=all
```

Record the tested revision, exact commands, versions, selected/passed/failed/skipped totals, reasons, outstanding gates,
and log locations in this sprint's review.

## Risks

Late feature additions would displace integrated verification. Keep this sprint for defects and release closure;
record newly discovered feature work for a later release.

## Non-goals

No new feature families, speculative support promotion, unexplained skips, or removal of regression coverage.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
