# Sprint 60: V11 Hardening and Closeout

Status: in progress; full matrix recorded, closeout blocked by the foreachBatch owner failure; target: 2027-03-05.

## Sprint goal

Reconcile all V11 ledgers and publish release evidence without admitting unresolved scope.

## User-facing outcome

The PySpark 4.1 support claim is reproducible, documented, and safe to adopt; every non-supported family explains its
boundary and remedy.

## Implementation tasks

- [x] Reconcile the supported pinned API delta, target-only inventory, and transformation coverage for query operations, observations, and KLL/Theta; exact-profile capability guards pass.
- [x] Review public API/reference/compatibility pages and current development records against implementation and
  evidence. Correct stale UDF/sketch/geospatial claims, fill schema/reference navigation gaps, and repair missing
  local documentation links. See the [documentation review](../V11DocumentationReview.md).
- [x] Implement explicit geospatial namespaces and native/Sedona schema types under
  [P10192602](../../planning/P10192602.Geospatial-namespaces-and-native-types.plan.md). Apache native 4.1 ordinary/
  Connect and Sedona 3.5/4.0/4.1 ordinary/Connect parity pass in isolated sessions. Exact-4.1 transforms mixing
  Apache/Sedona helpers emit `PYSPARK-W2707`; use separate sessions and persisted WKB/SRID handoffs. Inline bridges
  are not provided. Managed Databricks/Databricks Connect parity remains open.
- [x] Run `make build`: formatting, Flake8, mypy, 2,675 tests / 373 skips, 155 follow-up tests / 7 skips, and package builds passed.
- [x] Run all six unfiltered integration lanes. Corrected Connect 3.5/4.0 passed 205/90 and 215/80 (passed/skipped);
  Connect 4.1 passed Iceberg 21, Delta 55/1, and general 234/61. Ordinary 3.5 and 4.0 each reported one
  foreachBatch owner failure; ordinary 4.1 reported 53 passed, 2 skipped, and the same failure. Full evidence and
  provider pins are in [Sprint 59](Sprint59_V11IntegrationMatrix.md#corrected-six-lane-matrix-2026-10-21).
- [x] Run the integration-enabled build after the matrix: `make build INTEGRATION=1 BACKEND=spark-connect41` passed
  formatting, Flake8, mypy, 2,681 main tests / 373 skips, 155 follow-up tests / 7 skips, package builds, and the
  Connect 4.1 Iceberg 21, Delta 55 / 1 skip, and general 234 / 61 selection. The default all-backend integration build
  remains blocked at the first ordinary lane by the recorded foreachBatch failure.
- [x] Review the six-lane evidence report; retain the foreachBatch mismatch as an explicit release blocker with its
  existing owner.
- [x] Complete Arrow UDF implementation and evidence: Connect batch parity, retained scalar-UDF regressions, both
  unfiltered 4.1 lanes, and `make build`. The ordinary lane exposes one independent foreachBatch handoff failure;
  grouped/analytic streaming and Connect streaming remain explicitly excluded.
- [x] Keep the default profile at `>=3.5,<4.1` because the required passing matrix is not yet complete; record the
  decision in the V11 ExecPlan.

## Acceptance

Every supported row has positive evidence and every gate has a reason and remedy; generated output is fresh; the full
integration-enabled build passes; the foreachBatch blocker is resolved by its owner; and the V11 retrospective is
complete.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`.
