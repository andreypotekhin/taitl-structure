# Sprint 30: V7 Binary Values and Encoding

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

## Governing Documents

`docs/dev/design/V7DeferredPySparkFamilies.md`, `docs/dev/specifications/V7BinaryEncoding.md`, and
`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
