# Sprint 32: V7 Deterministic Mode

Status: complete. Grouped `mode(value, deterministic=False)` shipped with portable deterministic tie behavior,
placement/orderability diagnostics, generated/online parity, catalog status, and live PySpark 3.5/4.0 evidence.

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

## Evidence

- Focused numeric aggregate and generated-rendering checks passed with 48 tests.
- Live integration evidence records PySpark 3.5 passing with 4 tests and 3 skips, and PySpark 4.0 passing with 7 tests
  for `tests/integration/pyspark/v7/test_deterministic_mode.py`.

## Governing Documents

`docs/dev/design/V7DeferredPySparkFamilies.md`, `docs/dev/specifications/V7DeterministicMode.md`, and
`docs/dev/planning/done/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
