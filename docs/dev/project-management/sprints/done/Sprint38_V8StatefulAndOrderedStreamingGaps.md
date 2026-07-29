# Sprint 38: V8 Stateful and Ordered Streaming Gaps

Status: complete locally. The v8 design gate rejects arbitrary ordering, limits, offsets, and priority selection for
caller-owned Structured Streaming instead of admitting a narrow stateful shape. The checked ledger marks
`dataframe.ordering` and `dataframe.priority-selection` as `streaming-ineligible`.

## Sprint Goal

Resolve the remaining stateful or order-sensitive coverage gaps without overclaiming Spark behavior.

## User-Facing Outcome

The streaming report gives a precise answer for ordering and priority-selection shapes: supported with named
watermark/output-mode constraints, or rejected before query start with a specific alternative.

## In Scope

- Design gate for post-aggregate complete-mode ordering.
- Design gate for watermarked selected-row or priority-selection semantics.
- Diagnostics for arbitrary streaming `order_by(...)`, `limit(...)`, `offset(...)`, and row-number priority selection
  when they remain unsafe.
- Live PySpark 3.5/4.0 restart evidence for any admitted narrow stateful shape.

## Out of Scope

- Two-stateful chains.
- General global ordering over an unbounded input stream.
- Generated source/sink or output-mode ownership.
- Spark Connect streaming.

## Acceptance

No ordering or priority-selection operation remains ambiguous. Every accepted shape has restart evidence and every
rejected shape has a corrective diagnostic linked to the streaming documentation.

## Governing Documents

`docs/dev/specifications/V8StructuredStreamingCoverageParity.md` and
`docs/dev/planning/done/P07292601.V8-pyspark-structured-streaming-coverage-parity.plan.md`
