# Sprint 32: V7 Deterministic Mode

## Sprint Goal

Provide PySpark-named grouped `mode(...)` with deterministic results on both supported classic target lines.

## In Scope

- `mode(value, deterministic=False)` after `group_by(...)`, using the native PySpark 4.0 deterministic argument when
  available and a typed 3.5 compatibility aggregate otherwise.
- Tie/missing/null/orderability diagnostics, traceability, explain, generated/online parity, and classic-PySpark
  3.5/4.0 evidence.

## Out of Scope

- Global/unbounded streaming mode, implicit deterministic tie selection, and a raw scalar wrapper around PySpark `mode`.

## Acceptance

- A grouped same-frequency input returns the lowest orderable candidate with `deterministic=True` on both target lines;
  the non-deterministic default retains PySpark's ordinary tied-value behavior.

## Governing Documents

`docs/dev/design/V7DeferredPySparkFamilies.md`, `docs/dev/specifications/V7DeterministicMode.md`, and
`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
