# Configuration

Structure works by convention and supports a small TOML configuration for project-wide settings.

Use configuration for paths, package names, validation defaults, Spark SQL assumptions, target PySpark version,
compiler lineage settings, performance policy, compatibility behavior, and build behavior.

## Defaults

All defaults are declared in `pyproject.seed.toml`. User projects usually only need to specify settings that differ.

## pyproject.toml

Preferred:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
```

## structure.toml

Alternative:

```toml
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
```

## Path Settings

```toml
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
```

`source_roots` is an ordered list of filesystem import roots. Each root contains importable Python packages or modules.
`generated_dir` is the generated-code filesystem root.

`generated_package` is the Python package below `generated_dir` that owns generated Structure artifacts.

Recommended layout:

```text
src/orders/...
generated/structure_generated/orders/...
```

Generated modules mirror source import paths below `generated_package`. For example, source module
`src/orders/transforms/order.py` generates below `generated/structure_generated/orders/...`.

If no configuration is present, Structure resolves source roots by convention:

1. If `./src` exists and contains importable packages or modules, use `["src"]`.
2. Otherwise, use `["."]`.

Explicit configuration always wins.

IDE guidance:

- Mark every configured `source_roots` entry as a source root.
- Mark `generated` as a source root if you want generated-code navigation.
- Do not create a project package named `structure` unless you intend to shadow the installed Structure library.

## Validation Settings

```toml
validate_inputs = true
validate_intermediate = true
intermediate_validation_mode = "schema_only"
validate_outputs = true
```

Intermediate validation is enabled by default because subtransform return types define intermediate schemas.
Set `validate_intermediate = false` to disable intermediate schema validation for generated subtransform boundaries.

`intermediate_validation_mode` controls the cost and depth of enabled intermediate validation:

```text
schema_only
schema_and_constraints
```

Default: `schema_only`.

`schema_only` validates schema shape only: column names, data types, nullable flags where Spark exposes them reliably,
nested struct shape, and missing or extra columns. It must not trigger row scans.

`schema_and_constraints` may add row-level constraint checks when Structure supports them. Use it deliberately on
pipelines where the additional Spark work is worth the stronger runtime contract.

## Spark SQL Settings

```toml
spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"
```

Structure records Spark SQL assumptions using Spark's own dotted key names. These settings guide compile-time
nullability and type-coercion checks and document what generated runtime code expects from the caller's Spark session.

Structure does not create or reconfigure Spark sessions in v1.

## Compatibility Settings

```toml
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
```

`target_backend` selects the generated runtime backend. v1 supports `pyspark`.

`target_pyspark` constrains which PySpark APIs generated code may use. The v1 default targets PySpark 3.5.x and 4.0.x.
If a DSL feature cannot be generated for the configured range, `structure check` and `structure compile` should fail
with a diagnostic that names the required PySpark version.

Spark Connect is scheduled for v3 unless it can be added earlier without changing the public DSL, generated class API,
or generated-code review model. See `docs/Compatibility.md`.

## Lineage Settings

```toml
lineage = "compiler"
```

Supported lineage levels:

```text
none
compiler
columns
debug
```

Default: `compiler`.

`compiler` records source-to-IR-to-generated provenance and compact static dataflow dependencies. `columns` adds
field-level static dependencies where the compiler can infer them. `debug` may include fuller expression trees and
source locations for troubleshooting.

## Performance Policy

```toml
strict_performance = true
allow_python_udf = false
allow_pandas_udf = false
allow_rdd = false
allow_collect = false
allow_to_pandas = false
```

Compiled subtransforms never silently fall back to UDFs. These settings are primarily for hook linting and future
advanced features.

## Compile-Time Performance

```toml
incremental_compile = true
compiler_cache_dir = ".structure/cache"
parallel_codegen = true
```

Structure should be fast enough to run in local development and CI.

## Build Settings

```toml
format_generated = true
fail_on_diff = false
```

Use `fail_on_diff = true` in CI to ensure generated code is committed and up to date.
