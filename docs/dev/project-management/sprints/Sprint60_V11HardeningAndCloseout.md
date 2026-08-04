# Sprint 60: V11 Hardening and Closeout

Status: planned; target: 2027-03-05.

## Sprint goal

Reconcile all V11 ledgers and publish release evidence without admitting unresolved scope.

## User-facing outcome

The PySpark 4.1 support claim is reproducible, documented, and safe to adopt; every non-supported family explains its
boundary and remedy.

## Implementation tasks

- Reconcile API Catalog, API Reference, machine inventories, capabilities, diagnostics, compatibility docs, and generated artifacts.
- Run `make build`, `make integration`, and `make build INTEGRATION=1`.
- Review the six-lane evidence report and resolve release blockers or record precise deferrals.
- Decide whether the default profile may include 4.1 and record the decision in the V11 ExecPlan.

## Acceptance

Every supported row has positive evidence and every gate has a reason and remedy; generated output is fresh; the full
build passes; and the V11 retrospective is complete.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`.
