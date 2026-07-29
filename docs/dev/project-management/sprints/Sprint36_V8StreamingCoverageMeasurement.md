# Sprint 36: V8 Streaming Coverage Measurement

## Sprint Goal

Create the checked PySpark Structured Streaming coverage ledger and make the v8 percentage target enforceable.

## User-Facing Outcome

A developer can inspect one ledger and see whether each batch-supported PySpark family is streaming-supported,
partially supported, ineligible, or deferred, with evidence and a corrective note.

## In Scope

- Streaming coverage ledger resource.
- Guard tests that classify every batch-supported catalog row.
- Ratio calculation for batch coverage, family-level streaming coverage, and batch-supported-family streaming coverage.
- Initial operation-level split rules for mixed families.

## Starting Baseline

- Batch coverage: 34 / 36 catalog families, or 94.4 percent.
- Streaming coverage: 30 / 36 catalog families, or 83.3 percent.
- Streaming coverage among batch-supported families: 30 / 34, or 88.2 percent.
- Initial batch-supported streaming gaps: `functions.generators`, `dataframe.set`, `dataframe.ordering`, and
  `dataframe.priority-selection`.

## Out of Scope

- New streaming operation admission.
- Generated streaming source, sink, trigger, checkpoint, output-mode, or query lifecycle ownership.
- Spark Connect streaming.

## Acceptance

The new compatibility test passes, reports the kickoff measurement, and fails if a supported batch catalog family lacks
a streaming classification or evidence.

## Governing Documents

`docs/dev/specifications/V8StructuredStreamingCoverageParity.md` and
`docs/dev/planning/P07292601.V8-pyspark-structured-streaming-coverage-parity.plan.md`
