# Sprint 60: V11 Hardening and Closeout

Status: integration matrix and integration-enabled build passed on 2026-10-23; default profile promoted to `>=3.5,<4.2`; retrospective remains.

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
- [x] Run `make build`: formatting, Flake8, mypy, 2,682 main tests / 373 skips, 155 follow-up tests / 7 skips, and package builds passed after default-profile promotion.
- [x] Validate default-profile/configuration changes with 449 focused tests covering config resolution, capability selection,
  CLI templates, compatibility docs, plugin artifacts, state profiles, and backend metadata.
- [x] Rerun all six unfiltered integration lanes after the foreachBatch expectation was corrected. All ordinary and
  Connect phases passed; the corrected ordinary handoff case passed on 3.5, 4.0, and 4.1. Exact selection counts and
  skip reasons are in [Sprint 59](Sprint59_V11IntegrationMatrix.md#passing-closeout-rerun-2026-10-23).
- [x] Run `make build INTEGRATION=1` across all six backends. Formatting, Flake8, mypy, main and regression suites,
  package builds, provider phases, and every general selection passed. The complete output is retained in the
  integration workspace and summarized in Sprint 59.
- [x] Reconcile the six-lane evidence report; the 2026-10-21 foreachBatch failure is historical and resolved by the
  owner's `Alert` expectation correction.
- [x] Complete Arrow UDF implementation and evidence: Connect batch parity, retained scalar-UDF regressions, both
  unfiltered 4.1 lanes, and `make build`. Grouped/analytic streaming and Connect streaming remain explicitly excluded.
- [x] Record the release decision: promote the default profile to `>=3.5,<4.2`, based on the passing six-lane matrix
  and integration-enabled build.
- [x] Implement the [profile diagnostics and runtime validation follow-up](../../planning/P10232601.Profile-diagnostics-and-runtime-version-checks.plan.md):
  capability remedies now name applicable profiles and variants, rendered exceptions retain their explanation and
  documentation links, and a configurable client/engine version guard runs before execution and in generated classes.
  `make build` passed (2,695 main tests / 373 skips; 155 follow-up tests / 7 skips), and the runtime-profile case
  passed on all six pinned ordinary/Connect targets. The corrected generated-hook parity case passed on ordinary
  3.5.3. An unfiltered ordinary 3.5 rerun passed Iceberg and Delta, then hit the 3,600-second general-phase timeout
  at 52%; full-suite live validation remains open. See the [ExecPlan](../../planning/P10232601.Profile-diagnostics-and-runtime-version-checks.plan.md)
  for the exact evidence and disposition.

## Acceptance

Every supported row has positive evidence and every gate has a reason and remedy; generated output is fresh; the full
integration-enabled build passes; the foreachBatch blocker is resolved by its owner. The default-profile promotion is
recorded; the V11 retrospective remains to write.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`.
