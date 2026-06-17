# Configuration

Structure works by convention and supports a small TOML configuration for project-wide settings.

Use configuration for paths, package names, validation defaults, target PySpark version, lineage settings, performance policy, and build behavior.

## Defaults

All defaults are declared in `pyproject.seed.toml`. User projects usually only need to specify settings that differ.

## pyproject.toml

Preferred:

```toml
[tool.structure]
source_dir = "structure/src"
generated_dir = "structure/generated"
source_package = "pipeline_src"
generated_package = "pipeline_generated"
```

## structure.toml

Alternative:

```toml
source_dir = "structure/src"
generated_dir = "structure/generated"
source_package = "pipeline_src"
generated_package = "pipeline_generated"
```

## Path Settings

```toml
source_dir = "structure/src"
generated_dir = "structure/generated"
source_package = "pipeline_src"
generated_package = "pipeline_generated"
```

`source_dir` and `generated_dir` are filesystem roots.

`source_package` and `generated_package` are Python packages below those roots.

Recommended layout:

```text
structure/src/pipeline_src/...
structure/generated/pipeline_generated/...
```

IDE guidance:

- Mark `structure/src` as a source root.
- Mark `structure/generated` as a source root if you want generated-code navigation.
- Do not add `structure/__init__.py`.
- If the local `structure/` directory interferes with imports in a particular tool, change `source_dir` and `generated_dir` to `_structure/src` and `_structure/generated` or another project-specific container.

## Validation Settings

```toml
validate_inputs = true
validate_intermediate = true
validate_outputs = true
```

Intermediate validation is enabled by default because subtransform return types define intermediate schemas.

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

Compiled subtransforms never silently fall back to UDFs. These settings are primarily for hook linting and future advanced features.

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
