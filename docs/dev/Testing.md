# Testing

Structure needs tests for the compiler, generated code, runtime behavior, and PySpark emitter compatibility.

## Test Layers

### 1. DSL Unit Tests

Test expression objects, schema declarations, decorators, and helper behavior without Spark.

Covers:

- field declarations
- expression construction
- `where(...)`
- `@expr_fn`
- hook metadata
- transform metadata

### 2. Configuration Tests

Test seed config, `pyproject.toml`, `structure.toml`, and CLI override behavior.

Covers:

- default `structure/src` and `structure/generated` paths
- config resolution order
- seed config generation
- fail-on-diff setting
- lineage level setting
- validation setting overrides

### 3. Discovery Tests

Test that source modules produce expected transform models.

Covers:

- `@transform` class discovery
- source-order method discovery
- input declarations
- ambiguous public method errors
- hook attachment with `@after(method)`
- config resolution

### 4. Symbolic Execution Tests

Test source transform methods against symbolic proxies.

Covers:

- projection IR
- filter IR
- join IR
- expression helper expansion
- unsupported operation errors
- type flow errors
- config workaround hints in errors

### 5. IR Tests

Test that compiler output matches expected `TransformPlan`.

These tests are fast and do not require Spark.

### 6. Generated Code Snapshot Tests

Generate code and compare to approved snapshots.

Rules:

- format before snapshot
- keep snapshots small
- test representative examples
- avoid brittle whitespace-only failures where possible

### 7. Syntax and Import Tests

Generated code must:

- parse with `ast.parse`
- import successfully
- expose expected generated classes and functions

### 8. PySpark Execution Tests

Run generated transforms with small DataFrames.

Covers:

- projection
- add columns
- drop columns
- filtering
- joins
- serial joins across N inputs
- hooks
- schema validation success
- schema validation failure
- generated convenience functions

### 9. Plan and Performance Guardrail Tests

Generated compiled paths must not contain:

- `udf`
- `pandas_udf`
- `rdd`
- `collect`
- `toPandas`
- row-wise Python maps

Use static generated-code scans for v1. Use plan-level checks sparingly because Spark plans are version-sensitive.

### 10. Multi-Version PySpark Tests

Run a CI matrix for supported PySpark versions.

Initial target:

```text
PySpark 3.5.x
PySpark 4.0.x
latest supported PySpark
```

The exact matrix should be adjusted as PySpark support policy evolves.

### 11. Build Integration Tests

Test:

```bash
structure check
structure compile
structure compile --fail-on-diff
structure init --seed-config
```

`--fail-on-diff` should:

1. generate into a temporary directory
2. compare with checked-in generated output
3. fail if different
4. report changed files

### 12. Lineage Tests

Test LDJSON lineage output.

Covers:

- basic transform events
- input events
- step events
- join events
- hook opacity events
- output events
- optional field-level events
- compact default output

## CI Pipeline

Recommended CI order:

```text
1. ruff check source
2. structure check
3. structure compile --fail-on-diff
4. pytest compiler tests
5. pytest generated-code tests
6. pytest PySpark execution tests
7. package build
```

## Test Fixtures

Recommended fixture categories:

```text
fixtures/
  simple_projection/
  filtering/
  add_drop_columns/
  hooks/
  joins_one/
  joins_serial_n/
  invalid_unsupported_python/
  invalid_type_flow/
  invalid_hook_signature/
  config_defaults/
  lineage_basic/
```

Each fixture should include:

- source schemas
- source transform
- expected IR
- expected generated code
- optional input/output data
- optional expected lineage LDJSON

## Golden Rules

- Tests should protect the performance contract.
- Tests should protect generated code readability.
- Tests should protect error-message usefulness.
- Tests should protect PySpark version compatibility through the emitter layer.
- Tests should ensure hook-free generated code remains clean.
- Tests should ensure user configuration can remain minimal.
