# Sprint 30: V7 Binary Values and Encoding

Status: complete. Binary fields and typed encoding helpers shipped with schema materialization, expression rendering,
online/generated parity, catalog status, and live PySpark 3.5/4.0 evidence.

## Sprint Goal

Add an exact public Binary type and compiler-visible base64/charset conversions.

## In Scope

- Binary field declaration and schema materialization.
- Typed `base64`, `unbase64`, `encode`, and `decode` expressions with literal charset options.
- Null/invalid-input semantics, diagnostics, capability records, generated/online parity, and live PySpark 3.5/4.0 evidence.

## Out of Scope

- File bytes, arbitrary codecs, driver byte conversion, encryption, compression, and Spark Connect claims without separate evidence.

## Acceptance

- The catalog marks binary encoding supported only after the exact Binary/nullability and invalid-input behavior is proven on both classic targets.

## Evidence

- Focused source/schema/expression checks passed with 173 tests, as recorded in the consolidated v7 plan.
- Live integration evidence records PySpark 3.5 passing with 4 tests and 3 skips, and PySpark 4.0 passing with 7 tests
  for `tests/integration/pyspark/v7/test_binary_encoding.py`.

## Governing Documents

`docs/dev/design/V7DeferredPySparkFamilies.md`, `docs/dev/specifications/V7BinaryEncoding.md`, and
`docs/dev/planning/done/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
