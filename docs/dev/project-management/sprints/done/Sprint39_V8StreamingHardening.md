# Sprint 39: V8 Streaming Hardening

Status: complete. Targeted live v8 restart evidence passes on classic PySpark 3.5 and 4.0 for typed struct generators
and stream-stream unions. `make build` passes. Full PySpark 3.5 integration is still blocked by unrelated Search
integration failures, so v8 release hardening records targeted evidence separately.

## Sprint Goal

Close v8 without adding new feature scope.

## User-Facing Outcome

The published v8 streaming coverage claim is auditable, current, and backed by full static, generated, online, and live
PySpark evidence.

## In Scope

- Refresh generated artifacts and documentation.
- Run the full focused streaming suite and live PySpark 3.5/4.0 lanes for every admitted streaming family.
- Verify generated-source lifecycle/action scans.
- Reconcile deferred and ineligible streaming rows into backlog or troubleshooting documentation.
- Run `make build`.

## Out of Scope

- New streaming operation support.
- Spark Connect streaming.
- Lifecycle ownership.

## Acceptance

The streaming coverage percentage is at least the batch coverage percentage under the checked v8 measurement rule, all
release evidence is recorded in the v8 ExecPlan, and `make build` passes.

## Governing Documents

`docs/dev/specifications/V8StructuredStreamingCoverageParity.md` and
`docs/dev/planning/done/P07292601.V8-pyspark-structured-streaming-coverage-parity.plan.md`
