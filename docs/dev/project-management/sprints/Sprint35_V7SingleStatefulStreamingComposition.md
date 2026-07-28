# Sprint 35: V7 Single-Stateful Streaming Composition

## Sprint Goal

Allow one existing, bounded stateful streaming operation followed only by stateless transformations.

## In Scope

- A design-gated composition of one watermarked dedupe, window/session aggregate, or bounded stream-stream join with downstream stateless projection, filtering, or static enrichment.
- Output-mode inheritance, watermark/state explain records, diagnostics, generated-source scans, and file-stream restart evidence on PySpark 3.5/4.0.

## Out of Scope

- Two-stateful chains, generators, ordering/limits, analytic windows, source/sink ownership, and Spark Connect streaming.

## Acceptance

- The compiler accepts only the proven composition, preserves the upstream operation's output-mode rule, and fails early for a second stateful operation.

## Governing Documents

`docs/dev/design/V7CallerOwnedStreamingAdoption.md`, `docs/dev/specifications/V7CallerOwnedStreamingAdoption.md`, and
`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
