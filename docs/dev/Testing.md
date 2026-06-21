# Testing

Structure requires layered testing because correctness spans source DSL semantics, online execution, generated code,
runtime behavior, and performance guardrails.

## Test Layers

1. DSL unit tests.
2. Schema model tests.
3. Configuration validation tests.
4. Discovery tests.
5. Symbolic execution tests.
6. IR tests.
7. Compileability checker tests.
8. Negative compiler and diagnostic tests.
9. Online execution tests.
10. Generated-code snapshot tests.
11. Syntax/import tests.
12. PySpark execution tests.
13. Online/generated parity tests.
14. Performance guardrail tests.
15. Compile-time performance benchmarks.

## Generated-Code Correctness

Generated code should be tested by:

- snapshot comparison
- `ast.parse`
- import execution
- small Spark DataFrame input/output tests
- schema validation failure tests
- compiler provenance and static dataflow lineage tests

## Online Execution Correctness

Online execution should be tested by:

- config defaults and invalid execution-mode diagnostics
- transform invocation input binding
- deferred construction without Spark work
- `StructureSession.run(...)` delegation
- online PySpark execution against small Spark DataFrames
- parity with generated PySpark output for every supported v1 operation

## Online/Generated Parity

Every supported compiled operation must have at least one parity test before the operation is considered complete.
Parity tests run the same transform online through `StructureSession` and through the generated PySpark class, then
compare output column order, row contents, schema shape where Spark exposes it reliably, and expected validation
placement.

Generated-code snapshots are still required for reviewability, but snapshots are secondary. The semantic authority is
runtime parity through the shared contract in `docs/specifications/ExecutionSemanticContract.md`.

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
online execution, generated-code import, and PySpark execution tests in separate suites because those may legitimately
require PySpark and a local Spark runtime.

## Test Placement

Use these directories consistently:

- `tests/app/[app]/[subapp]/...`: tests for app implementation code. Keep nesting aligned with the app and subapp
  package path.
- `tests/specs/[section-or-story]/...`: tests backing user stories from `docs/dev/Specification.md`.
- `tests/specifications/[specification-doc-slug]/...`: tests backing individual documents under `docs/specifications/`
  when we need to prove the behavior described by a specification document directly.

Examples:

- CLI command behavior: `tests/app/cli/...`
- Backend capability app behavior: `tests/app/backend/capabilities/...`
- PySpark backend target behavior: `tests/app/backend/pyspark/...`
- User stories completed from `docs/dev/Specification.md`: `tests/specs/...`
- Execution semantic contract checks: `tests/specifications/execution-semantic-contract/...`
- PySpark code generation contract checks: `tests/specifications/pyspark-code-generation/...`

## CI

Recommended CI pipeline:

```text
1. ruff check
2. structure check
3. structure compile --fail-on-diff
4. pytest compiler tests
5. pytest negative compiler and diagnostic tests
6. pytest online execution tests
7. pytest generated-code tests
8. pytest PySpark execution tests
9. pytest online/generated parity tests
10. compile-time benchmark smoke test
11. package build
```
