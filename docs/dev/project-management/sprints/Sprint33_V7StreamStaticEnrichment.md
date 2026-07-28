# Sprint 33: V7 Stream-Static Enrichment

## Sprint Goal

Admit caller-owned stream-static inner, left, and left-semi enrichment with no new streaming state.

## In Scope

- A streaming current relation on the left and an explicitly static lookup relation on the right.
- Unique/deterministically deduped lookup keys, static diagnostics, explain, generated-source lifecycle scans, and file-stream restart evidence on PySpark 3.5/4.0.

## Out of Scope

- Right/full/cross/anti directions, streaming lookup relations, lifecycle ownership, and stateful composition.

## Acceptance

- Online and generated transforms return streaming DataFrame plans while the caller continues to own query setup and restart.

## Governing Documents

`docs/dev/design/V7CallerOwnedStreamingAdoption.md`, `docs/dev/specifications/V7CallerOwnedStreamingAdoption.md`, and
`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
