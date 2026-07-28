# Sprint 34: V7 Stream-Static Outer Lookup

## Sprint Goal

Extend caller-owned enrichment with left-outer static lookup semantics and declared nullable lookup fields.

## In Scope

- Left-outer lookup schema/nullability rules, static diagnostics, explain, parity, generated-source scans, and file-stream restart evidence on PySpark 3.5/4.0.

## Out of Scope

- Any reverse outer direction, stateful composition, and lifecycle ownership.

## Acceptance

- Unmatched streaming rows are preserved and receive only the declared nullable lookup fields.

## Governing Documents

`docs/dev/design/V7CallerOwnedStreamingAdoption.md`, `docs/dev/specifications/V7CallerOwnedStreamingAdoption.md`, and
`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
