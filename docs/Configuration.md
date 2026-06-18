# Configuration

Structure works by convention and supports a small TOML configuration for project-wide settings.

Use configuration for paths, package names, validation defaults, Spark SQL assumptions, target PySpark version,
lineage settings, performance policy, and build behavior.

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
src/pipeline_src/...
generated/structure_generated/pipeline_src/...
```

Generated modules mirror source import paths below `generated_package`. For example, source module
`src/pipeline_src/transforms/order.py` generates below `generated/structure_generated/pipeline_src/...`.

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
validate_outputs = true
```

Intermediate validation is enabled by default because subtransform return types define intermediate schemas.

## Spark SQL Settings

```toml
spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"
```

Structure records Spark SQL assumptions using Spark's own dotted key names. These settings guide compile-time
nullability and type-coercion checks and document what generated runtime code expects from the caller's Spark session.

Structure does not create or reconfigure Spark sessions in v1.

## Lineage Settings

```toml
lineage = "basic"
lineage_format = "ldjson"
```

Supported lineage levels:

```text
none
basic
fields
debug
```

Default: `basic`.

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
