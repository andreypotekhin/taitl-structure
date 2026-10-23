# Outstanding Work

This is the current follow-up index. Feature status is maintained in the
[API tracker](../../compatibility/APITracker.md); release acceptance is maintained in [Milestones](Milestones.md).

## V11 release checks

- Rerun the ordinary 3.5, 4.0, and 4.1 lanes after the foreachBatch owner correction, then complete the all-backend
  `make build INTEGRATION=1`. The recorded 2026-10-21 runs each failed the same handoff-schema assertion; the working
  tree now corrects the expected schema to the declared `Alert`, but that edit does not replace live evidence.
- Keep the default profile at `>=3.5,<4.1` until the required passing matrix is complete. Connect 3.5/4.0/4.1 passed
  their latest configured selections; ordinary 4.1 remains scoped to the runtime assertion and V11 tests.
- Reconcile the final release report and retrospective after those results. See
  [Sprint 59](sprints/Sprint59_V11IntegrationMatrix.md)
  and [Sprint 60](sprints/Sprint60_V11HardeningAndCloseout.md).

## Feature evidence gates

- Managed Databricks Spark and Databricks Connect require caller-supplied live sessions. Local Apache/Sedona parity
  does not establish managed-runtime support. Apache/Sedona workloads use separate sessions; mixed exact-4.1 transforms
  emit `PYSPARK-W2707`. See the [Geospatial plan](../planning/P10192602.Geospatial-namespaces-and-native-types.plan.md).
- Legacy `apply_in_pandas_with_state` has accumulation/restart evidence, with timeout and zero/multiple-output proof
  still pending under its [own plan](../planning/P10062602.V11-apply-in-pandas-with-state.plan.md).
- Broader stateful chains, streaming missing-column union, unreleased Variant operations, XML, and join reordering
  retain their individual dispositions in [API gates](../gated/ApiCatalog.gates.md),
  [Streaming gates](../gated/Streaming.gates.md), and [Function gates](../gated/Functions.gates.md).

## Earlier release requirements

The earlier milestone records still identify CI workflow evidence, broader negative live-schema cases, generated
version/target headers, and setup/doctor checks as follow-up work. Reconcile each against its implementation and
specific acceptance evidence before closing M0–M6; V11 feature completion does not close those records automatically.
See [Milestones](Milestones.md) and [Compatibility](../../Compatibility.md).
