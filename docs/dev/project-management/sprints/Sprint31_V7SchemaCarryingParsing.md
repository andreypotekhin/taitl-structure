# Sprint 31: V7 Schema-Carrying JSON and CSV Parsing

## Sprint Goal

Add exact Schema-carrying JSON/CSV conversion with a normalized, compiler-visible option record.

## In Scope

- Typed JSON/CSV parse and render helpers using declared Structure Schemas.
- Immutable literal options for delimiter, quote, escape, null value, date/timestamp formats, and permissive mode.
- Parse-failure/nullability diagnostics, capability records, generated/online parity, and classic-PySpark 3.5/4.0 evidence.

## Out of Scope

- Schema inference, arbitrary option dictionaries, map/variant parser outputs, dynamic options, file reads, and streaming parser claims.

## Acceptance

- Every admitted conversion has an exact result schema and documented target-consistent parse-failure behavior.

## Governing Documents

`docs/dev/design/V7DeferredPySparkFamilies.md`, `docs/dev/specifications/V7SchemaCarryingParsing.md`, and
`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
