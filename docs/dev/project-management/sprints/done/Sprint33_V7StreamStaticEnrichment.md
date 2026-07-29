# Sprint 33: V7 Stream-Static Enrichment

Status: complete. Stage One caller-owned stream-static inner, left, and left-semi enrichment shipped with restart
evidence and generated-source lifecycle guards.

## Sprint Goal

Admit caller-owned stream-static inner, left, and left-semi enrichment with no new streaming state.

## In Scope

- A streaming current relation on the left and an explicitly static lookup relation on the right.
- Unique/deterministically deduped lookup keys, static diagnostics, explain, generated-source lifecycle scans, and file-stream restart evidence on PySpark 3.5/4.0.

## Out of Scope

- Right/full/cross/anti directions, streaming lookup relations, lifecycle ownership, and stateful composition.

## Acceptance

- Online and generated transforms return streaming DataFrame plans while the caller continues to own query setup and restart.

## Evidence

- Live integration evidence records PySpark 3.5 passing with 4 tests and 3 skips, and PySpark 4.0 passing with 7 tests
  for `tests/integration/pyspark/v7/test_stream_static_restart.py`.
- The fixture asserts generated transform source contains no streaming source, sink, checkpoint, trigger, output-mode,
  action, or query-lifecycle ownership calls.

## Governing Documents

`docs/dev/design/V7CallerOwnedStreamingAdoption.md`, `docs/dev/specifications/V7CallerOwnedStreamingAdoption.md`, and
`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
