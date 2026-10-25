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

## Profile diagnostics and runtime validation

- [ ] Finish full live-suite validation for the implemented [profile diagnostics and runtime version checks ExecPlan](../planning/P10232601.Profile-diagnostics-and-runtime-version-checks.plan.md).
  `make build` and the focused runtime-profile case on all six pinned ordinary/Connect targets pass. The corrected
  generated-hook parity case passes on ordinary 3.5.3. Its unfiltered general phase timed out after 3,600 seconds
  at 52%; the full six-lane suites remain open. The default-profile selection discussion remains separate; this
  plan retains `>=3.5,<4.2` and existing capability admissions.

## Feature evidence gates

- Managed Databricks Spark and Databricks Connect require caller-supplied live sessions. Local Apache/Sedona parity
  does not establish managed-runtime support. Apache/Sedona workloads use separate sessions; mixed exact-4.1 transforms
  emit `PYSPARK-W2707`. See the [Geospatial plan](../planning/P10192602.Geospatial-namespaces-and-native-types.plan.md).
- Legacy `apply_in_pandas_with_state` has accumulation/restart evidence, with timeout and zero/multiple-output proof
  still pending under its [own plan](../planning/P10062602.V11-apply-in-pandas-with-state.plan.md).
- Broader stateful chains, streaming missing-column union, unreleased Variant operations, XML, and join reordering
  retain their individual dispositions in [API gates](../gated/ApiCatalog.gates.md),
  [Streaming gates](../gated/Streaming.gates.md), and [Function gates](../gated/Functions.gates.md).

## V12 planned delivery

The [V12 charter](V12.md) and [backlog](Backlog.md#v12) track twelve planned sprints, 61–72. These tasks do not alter
current support claims or erase the V11 follow-ups above.

- [ ] Establish the 4.2 inventory, two runtime lanes, V11 continuity map, and provider feasibility in Sprint 61.
- [ ] Deliver the temporal, spatial, sketch, query/ranking, and Arrow packages in Sprints 62–67.
- [ ] Deliver ordinary state completion, streaming composition, and streaming observations in Sprints 68–70.
  Spark Connect remains batch-only.
- [ ] Prove available table-provider/CDC configurations in Sprint 71 using Sprint 61's dependency findings.
- [ ] Close M19 through dedicated Sprint 72 hardening, eight documented runtime selections, and the
  integration-enabled build. Set calendar dates separately; retain the 24-week relative planning envelope.

## Earlier release requirements

The earlier milestone records still identify CI workflow evidence, broader negative live-schema cases, generated
version/target headers, and setup/doctor checks as follow-up work. Reconcile each against its implementation and
specific acceptance evidence before closing M0–M6; V11 feature completion does not close those records automatically.
See [Milestones](Milestones.md) and [Compatibility](../../Compatibility.md).
