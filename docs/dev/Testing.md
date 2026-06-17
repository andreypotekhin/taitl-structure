# Testing

Structure requires layered testing because correctness spans source DSL semantics, generated code, runtime behavior, and performance guardrails.

## Test Layers

1. DSL unit tests.
2. Schema model tests.
3. Discovery tests.
4. Symbolic execution tests.
5. IR tests.
6. Compileability checker tests.
7. Generated-code snapshot tests.
8. Syntax/import tests.
9. PySpark execution tests.
10. Performance guardrail tests.
11. Compile-time performance benchmarks.

## Generated-Code Correctness

Generated code should be tested by:

- snapshot comparison
- `ast.parse`
- import execution
- small Spark DataFrame input/output tests
- schema validation failure tests
- lineage output tests

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

Test cold compile and warm incremental compile separately.

Warm incremental compile should avoid symbolic execution and regeneration for unchanged transforms.

## CI

Recommended CI pipeline:

```text
1. ruff check
2. structure check
3. structure compile --fail-on-diff
4. pytest compiler tests
5. pytest generated-code tests
6. pytest PySpark execution tests
7. compile-time benchmark smoke test
8. package build
```
