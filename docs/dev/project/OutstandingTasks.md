# Outstanding Work

This is the current follow-up index. Feature status is maintained in the
[API tracker](../../compatibility/APITracker.md); release acceptance is maintained in [Milestones](Milestones.md).

## V11 release checks

- [x] Rerun the ordinary 3.5, 4.0, and 4.1 lanes after the foreachBatch owner correction.
- [x] Complete `make build INTEGRATION=1` across all six backend lanes; every selected phase passed.
- [x] Promote the default profile to `>=3.5,<4.2` after the six-lane matrix and integration-enabled build passed on
  2026-10-23. Exact results and skip reasons are in
  [Sprint 59](sprints/Sprint59_V11IntegrationMatrix.md#passing-closeout-rerun-2026-10-23) and
  [Sprint 60](sprints/Sprint60_V11HardeningAndCloseout.md).
- [ ] Write the V11 retrospective. The profile decision is recorded in the V11 charter and Sprint 60.

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
