# Implementation

## Phase 1: Minimal Vertical Slice

- Schema declarations.
- `input(...)`.
- `@transform` discovery.
- Single schema-returning subtransform.
- Symbolic field refs.
- Projection generation.
- Spark `StructType` generation.
- Generated transform class.
- Input and output validation.
- CLI `check` and `compile`.

## Phase 2: v1 Complete

- Source-order multi-subtransform chains.
- Intermediate validation.
- `where(...)` filtering.
- `@expr_fn` helpers.
- `@after(method)` and `@before(method)` hooks.
- Hook signature validation.
- `join_one(...)`.
- N-step serial joins.
- Clean no-hook generated code.
- Structured compiler errors.
- Streaming compatibility checks.
- Basic LDJSON lineage.
- TOML configuration.
- Incremental compile.

## Phase 3: v2

- Aggregations.
- Advanced grouping.
- Windowing.
- Spark higher-order functions.
- Caching and persistence annotations.
- Join strategy annotations.
- Optional field-level lineage.

## Phase 4: v3

- Generated stream reads/writes.
- Watermarks.
- Triggers.
- Checkpoints.
- Streaming lifecycle configuration.

## Build Integration

Initial build integration should rely on CLI commands:

```bash
structure check
structure compile
structure compile --fail-on-diff
```

Later, add optional pytest and build-tool integrations.

## Compile-Time Performance Metrics

Track:

- config load time
- module discovery time
- source inspection time
- symbolic execution time
- check time
- codegen time
- formatting time
- lineage time
- files regenerated
- cache hit ratio
- total wall-clock time

Add `structure compile --profile` to emit these measurements.
