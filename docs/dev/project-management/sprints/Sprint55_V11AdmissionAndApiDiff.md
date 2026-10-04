# Sprint 55: V11 Admission and PySpark 4.1 API Diff

Status: planned; target: 2026-12-25.

## Sprint goal

Turn the PySpark 4.1 release delta into one reviewed, versioned Structure ledger and finish implementation-ready
design/specification ownership for every feature family.

## User-facing outcome

Contributors can look up any reviewed 4.1 addition and find one status, contract owner, diagnostic, test location, and
evidence path.

## Implementation tasks

- Add the 4.1-to-4.0 API inventory and reconcile machine-readable coverage.
- Add exact 4.1 target profile and variant policy tests.
- Review the V11 design/specification documents and record scope decisions.
- Update catalog/reference, roadmap, milestone, backlog, and traceability navigation.

## Inherited V10 entry conditions

Sprint 55 owns the initial classification and handoff of the unresolved V10 evidence; these items are not silently
treated as passing baselines:

| Handoff item | Sprint 55 owner | Entry condition and acceptance |
| --- | --- | --- |
| V1/V2 order-hook schema failures: generated relations omit physical `promo-code` | V1/V2 order-hook compatibility owner | Reproduce the failure, correct it or record an accepted carry-forward owner, and keep the regression outside the V11 feature claim. |
| Four V11 scalar-assertion generated-import failures | V11 generated-artifact/import owner | Reproduce the import-path failure, repair the generated-module contract or record the exact blocker, and add a no-Spark regression check. |
| Incomplete PySpark 4.0 and Spark Connect full-lane runs | V11 integration-matrix owner | Preserve the unavailable evidence status, define bounded rerun commands, and do not use focused results as suite-level pass evidence. |

The source baseline is [V10 Release Evidence](../V10ReleaseEvidence.md), dated 2026-10-04. Sprint 55 may proceed with
feature inventory work while these owners classify the handoff, but Sprint 55 cannot close with an unresolved item
having no owner, command, status, or next decision.

## Acceptance

No reviewed 4.1 row is unclassified, every inherited V10 item has an owner and disposition, and `make build` remains
Spark-free and green.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`.
