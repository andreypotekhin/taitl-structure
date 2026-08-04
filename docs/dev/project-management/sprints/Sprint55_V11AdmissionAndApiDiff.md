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

## Acceptance

No reviewed 4.1 row is unclassified, and `make build` remains Spark-free and green.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`.
