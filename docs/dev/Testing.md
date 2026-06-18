# Testing

Structure requires layered testing because correctness spans source DSL semantics, generated code, runtime behavior, and performance guardrails.

## Test Layers

1. DSL unit tests.
2. Schema model tests.
3. Configuration validation tests.
4. Discovery tests.
5. Symbolic execution tests.
6. IR tests.
7. Compileability checker tests.
8. Negative compiler and diagnostic tests.
9. Generated-code snapshot tests.
10. Syntax/import tests.
11. PySpark execution tests.
12. Performance guardrail tests.
13. Compile-time performance benchmarks.

## Generated-Code Correctness

Generated code should be tested by:

- snapshot comparison
- `ast.parse`
- import execution
- small Spark DataFrame input/output tests
- schema validation failure tests
- compiler provenance and static dataflow lineage tests

## Negative Compiler and Diagnostic Tests

Each supported DSL feature needs at least one intentionally broken transform test when it has a meaningful failure mode.
These tests should assert the diagnostic code, location, problem summary, and suggested fix, not merely that compilation
failed.

Required v1 negative cases:

- missing fields
- wrong types
- nullable-to-non-nullable assignment
- invalid hook signatures
- ambiguous public methods
- bad source order
- unsupported Python methods
- `join_one(...)` without uniqueness warning
- duplicate output fields
- non-boolean filters
- `@expr_fn` returning non-expression values

## Performance Guardrails

Compiled generated paths must not contain:

- `udf`
- `pandas_udf`
- `rdd`
- `collect`
- `toPandas`
- Python row maps

Hooks may use arbitrary PySpark, but strict performance mode should lint hooks and report risky operations.

## Compile-Time Performance Tests

Add benchmark fixtures for:

- 10 transforms
- 100 transforms
- 1,000 transforms
- N-step serial joins
- many schema files
- many expression helpers

Test cold compile in v1. Add separate cold and warm incremental-compile tests when v2 production incremental compile is
implemented.

Warm incremental compile should avoid symbolic execution and regeneration for unchanged transforms once the v2 cache is
enabled.

Compiler tests must prove the no-Spark compile contract: `structure check`, `structure compile`, and
`structure compile --fail-on-diff` run without PySpark, Java, a SparkSession, Spark startup, or a Spark cluster. Keep
generated-code import tests and PySpark execution tests in separate suites because those may legitimately require
PySpark and a local Spark runtime.

## CI

Recommended CI pipeline:

```text
1. ruff check
2. structure check
3. structure compile --fail-on-diff
4. pytest compiler tests
5. pytest negative compiler and diagnostic tests
6. pytest generated-code tests
7. pytest PySpark execution tests
8. compile-time benchmark smoke test
9. package build
```
