# Sprint 60: V11 Hardening and Closeout

Status: planned; target: 2027-03-05.

## Sprint goal

Reconcile all V11 ledgers and publish release evidence without admitting unresolved scope.

## User-facing outcome

The PySpark 4.1 support claim is reproducible, documented, and safe to adopt; every non-supported family explains its
boundary and remedy.

## Implementation tasks

- [x] Reconcile the supported pinned API delta, target-only inventory, and transformation coverage for query operations, observations, and KLL/Theta; exact-profile capability guards pass.
- Reconcile remaining API Catalog, API Reference, compatibility docs, generated artifacts, and six-lane evidence.
- [x] Implement explicit geospatial namespaces and native/Sedona schema types under
  [P10192602](../../planning/P10192602.Geospatial-namespaces-and-native-types.plan.md). Apache native 4.1 ordinary/
  Connect and Sedona 3.5/4.0/4.1 ordinary/Connect parity pass in isolated sessions. Same-session Apache/Sedona use,
  Binary bridges, and managed Databricks/Databricks Connect parity remain gated.
- [x] Run `make build`: formatting, Flake8, mypy, 2,675 tests / 373 skips, 155 follow-up tests / 7 skips, and package builds passed.
- [x] Run full ordinary and Connect 4.1 integration lanes. Connect passed Iceberg 21, Delta 55 / 1 expected skip,
  and general 234 / 60 skips. Ordinary passed Iceberg 21 and Delta 53 / 3 expected skips; general had 53 passes,
  one Databricks-session skip, and one independent foreachBatch schema failure (`AlertMessage` expected, `Alert` received).
- Run the complete six-lane integration matrix and `make build INTEGRATION=1`.
- Review the six-lane evidence report and resolve release blockers or record precise deferrals.
- [x] Complete Arrow UDF implementation and evidence: Connect batch parity, retained scalar-UDF regressions, both
  unfiltered 4.1 lanes, and `make build`. The ordinary lane exposes one independent foreachBatch handoff failure;
  grouped/analytic streaming and Connect streaming remain explicitly excluded.
- Decide whether the default profile may include 4.1 and record the decision in the V11 ExecPlan.

## Acceptance

Every supported row has positive evidence and every gate has a reason and remedy; generated output is fresh; the full
build passes; and the V11 retrospective is complete.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`.
