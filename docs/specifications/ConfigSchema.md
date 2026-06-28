# Configuration Schema

## Purpose

Structure configuration controls source discovery, generated output, execution mode, target backend, validation,
traceability, Spark SQL assumptions, and CI behavior. Configuration errors must fail early with structured diagnostics and
allowed values.

This specification owns configuration files, resolution order, keys, defaults, validation rules, diagnostics, and tests.

## Configuration Sources

Supported sources, lowest to highest precedence:

1. Built-in defaults.
2. `structure.toml`.
3. `[tool.structure]` in `pyproject.toml`.
4. CLI flags.

If both `structure.toml` and `[tool.structure]` exist, `[tool.structure]` wins for overlapping keys. Non-overlapping
keys merge unless a later decision requires a single authoritative file.

CLI flags override both files.

## Default Configuration

Seed defaults:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
traceability = "compiler"
validate_intermediate = true
input_validation_mode = "schema_only"
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
strict_performance = true
fail_on_diff = false

spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"
```

When no configuration file exists, source-root discovery may replace `source_roots = ["src"]` with `["."]` according
to [SourceModuleRules.md](SourceModuleRules.md).

## Keys

### source_roots

Type: list of strings.

Default: discovered by source-root rules.

Rules:

- Values are project-relative paths unless absolute paths are explicitly allowed later.
- The list must not be empty.
- Each path must exist by the time discovery runs.
- Paths must not be inside `generated_dir`.

### generated_dir

Type: string.

Default: `"generated"`.

Rules:

- Must be a project-relative directory path in v1.
- The compiler may create it during `structure compile`.
- `structure check` must not require it to exist.

### generated_package

Type: string.

Default: `"structure_generated"`.

Rules:

- Must be a valid dotted Python package name.
- Must not be `"structure"`.
- Must not collide with a discovered source package.

### execution_mode

Type: string enum.

Allowed:

```text
online
generated
```

Default: `"online"`.

### target_backend

Type: string enum.

Allowed in v1:

```text
pyspark
```

Unknown backends fail through backend capability diagnostics.

### target_pyspark

Type: version range string.

Default: `">=3.5,<4.1"`.

Rules:

- Must be parseable by the project's version range parser.
- Must resolve to a supported PySpark capability profile.
- Must not inspect the locally installed PySpark version during compiler commands.

### traceability

Type: string enum.

Allowed:

```text
none
compiler
columns
debug
```

Default: `"compiler"`.

`compiler` includes compiler provenance and static dataflow basics. `columns` and `debug` may be richer modes, but
must remain deterministic and documented before release.

### validate_intermediate

Type: boolean.

Default: `true`.

Compatibility shortcut for intermediate validation. Prefer `intermediate_validation_mode` for new docs and examples.

### input_validation_mode

Type: string enum.

Allowed:

```text
off
schema_only
schema_and_constraints
```

Default: `"schema_only"`.

### intermediate_validation_mode

Type: string enum.

Allowed:

```text
off
schema_only
schema_and_constraints
```

Default: `"schema_only"`.

### output_validation_mode

Type: string enum.

Allowed:

```text
off
schema_only
schema_and_constraints
```

Default: `"schema_only"`.

### strict_performance

Type: boolean.

Default: `true`.

Rules:

- When true, unsupported compiler-visible operations fail instead of silently becoming UDFs, row-wise callbacks, RDD
  operations, or opaque generated code.
- v1 docs should keep this true in examples.

### fail_on_diff

Type: boolean.

Default: `false`.

Rules:

- CLI `structure compile --fail-on-diff` overrides this to true.
- When true, compile checks generated output freshness and exits with a diagnostic if files would change.

### spark.sql.ansi.enabled

Type: boolean.

Default: `true`.

Compiler assumption used by nullability and type assignment rules.

### spark.sql.storeAssignmentPolicy

Type: string enum.

Allowed:

```text
ANSI
LEGACY
STRICT
```

Default: `"ANSI"`.

Detailed v1 assignment rules are specified for `ANSI`.

## Unknown Keys

Unknown keys are errors. Structure should suggest close known keys when the edit distance is small and the suggestion is
unambiguous.

Example:

```text
CompileError CONF-E0101: Unknown configuration key

Setting:
  [tool.structure].generatedDirectory

Problem:
  Structure does not define this configuration key.

Use:
  generated_dir = "generated"

See docs/specifications/ConfigSchema.md
```

## Invalid Values

Invalid values must include allowed values.

Example:

```text
CompileError CONF-E0102: Invalid configuration value

Setting:
  [tool.structure].traceability = "fieldz"

Allowed:
  none
  compiler
  columns
  debug

Use:
  traceability = "columns"

See docs/specifications/ConfigSchema.md
```

## Effective Config

The resolver must produce an immutable effective configuration object:

```text
StructureConfig
  project_root
  source_roots
  generated_dir
  generated_package
  execution_mode
  target_backend
  target_pyspark
  traceability
  validation
  strict_performance
  fail_on_diff
  spark_sql
  source_map
```

`source_map` records which file, default, or CLI flag supplied each final setting for diagnostics and explain output.

## Security

Configuration diagnostics must not print secrets. v1 Structure config should avoid secret-bearing fields. If future
settings can include credentials or tokens, diagnostics must redact values by default.

## Implementation Checklist

1. Implement TOML loading for `pyproject.toml` and `structure.toml`.
2. Extract `[tool.structure]` from `pyproject.toml`.
3. Merge defaults, `structure.toml`, `[tool.structure]`, and CLI flags in order.
4. Validate unknown keys.
5. Validate value types and enum values.
6. Normalize paths relative to project root.
7. Resolve source roots with `SourceModuleRules.md`.
8. Resolve backend capabilities without importing PySpark.
9. Normalize validation shortcuts and validation modes.
10. Produce immutable effective config with source metadata.
11. Add structured diagnostics and docs links.
12. Add tests for defaults, precedence, unknown keys, invalid values, path validation, and no-Spark resolution.

## Acceptance Criteria

- With no config, Structure resolves usable defaults and source roots.
- CLI flags override file configuration.
- `[tool.structure]` overrides `structure.toml` for overlapping keys.
- Unknown keys fail with suggestions when available.
- Invalid enum values show allowed values.
- Invalid types fail before discovery.
- `generated_package = "structure"` fails.
- `target_backend = "pyspark"` resolves without importing PySpark.
- Unknown `target_backend` fails through backend diagnostics.
- Validation mode shortcuts normalize deterministically.
- Effective config records the source of each setting.
