# Implementation

## Phase 1: Minimal Vertical Slice

- Schema declarations.
- `input(...)`.
- `@transform` discovery.
- Single schema-returning subtransform.
- Symbolic field refs.
- Projection generation.
- `StructureSession`.
- Builder-style transform invocation.
- Online PySpark execution of the minimal projection transform.
- Spark `StructType` generation.
- Generated transform class.
- Input and output validation.
- CLI `check` and `compile`.
- TOML config loading with explicit precedence and schema validation diagnostics.
- Compatibility policy enforcement for Python and target PySpark configuration.

## Phase 2: v1 Complete

- Online PySpark runner for all v1 transform operations.
- Runtime target registry for online and generated PySpark execution.
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
- Streaming compatibility report.
- Compiler provenance from source node to IR node to generated PySpark node.
- Static dataflow lineage inferred from IR.
- TOML configuration hardening.
- Diagnostic codes with documentation links.
- Setup/configuration doctor.
- Incremental-compile architecture hooks.

## Phase 3: v2

- Windowing.
- Deduplication helpers.
- Aggregations.
- Advanced grouping.
- Spark higher-order functions.
- Caching and persistence annotations.
- Repartition/coalesce annotations.
- Join strategy annotations.
- `join_many(...)` and other row-multiplying or existence-oriented join forms.
- Richer static dataflow explain output.
- Production incremental compile.
- Generated documentation artifacts for schemas and transforms.
- Pytest helper or plugin.

## Phase 4: v3

- Generated stream reads/writes.
- Watermarks.
- Triggers.
- Checkpoints.
- Output modes.
- Streaming lifecycle configuration.

## Phase 5: v4

- Spark Connect support.
- Spark Connect compatibility tests.
- Backend capability reporting for ordinary PySpark and Spark Connect targets.

## Build Integration

Initial build integration should rely on CLI commands:

```bash
structure check
structure compile
structure compile --fail-on-diff
```

Later, add optional pytest and build-tool integrations as v2 adoption tooling.

Compiler build integration must stay Spark-free. `structure check`, `structure compile`, and
`structure compile --fail-on-diff` must not require PySpark, Java, a SparkSession, or a Spark cluster. Online runtime,
generated-code import, and PySpark execution tests may require those dependencies and should remain separate from
compiler checks.

## Compile-Time Performance Metrics

Track:

- config load time
- module discovery time
- source inspection time
- symbolic execution time
- check time
- codegen time
- formatting time
- compiler provenance time
- static dataflow lineage time
- files regenerated
- cache hit ratio
- total wall-clock time

Add `structure compile --profile` to emit these measurements. Production incremental compilation belongs to v2; v1
should preserve deterministic outputs and source fingerprints so the cache can be added without reshaping the compiler.
