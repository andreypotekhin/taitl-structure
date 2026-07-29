# Sprint 44: V9 Streaming Hardening

Status: complete. V9 release evidence is recorded in the ExecPlan. `make build` passed on 2026-07-29 with 1,392
tests passed and 52 skipped in the main suite, then 44 passed and 6 skipped in the focused release subset, followed by
successful sdist and wheel builds.

## Sprint Goal

Close v9 release evidence without adding new streaming API scope.

## User-Facing Outcome

The v9 streaming API coverage claim is current, auditable, documented, and backed by static, generated, online, and
live PySpark evidence.

## In Scope

- Refresh generated artifacts and documentation.
- Run streaming API ledger tests, streaming compatibility tests, generated-source scans, and adoption recipe tests.
- Run live PySpark 3.5 and 4.0 streaming lanes for admitted claims.
- Reconcile any skipped or blocked live evidence into explicit deferrals.
- Run `make build`.

## Out of Scope

- New API support.
- Lifecycle ownership.
- Spark Connect streaming promotion.

## Acceptance

The v9 ExecPlan records final evidence, every v9 sprint outcome is reflected in the ledger and docs, no unsupported API
is counted as supported, and `make build` passes.

## Governing Documents

`docs/dev/specifications/V9PySparkStreamingApiCoverage.md` and
`docs/dev/planning/done/P07292602.V9-pyspark-streaming-api-coverage.plan.md`
