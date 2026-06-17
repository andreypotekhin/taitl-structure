# Configuration

Structure uses conventions by default and a small TOML configuration for project-wide settings.

Configuration is optional. Structure has a seed config containing all defaults. User projects only need to specify settings that differ from defaults.

## Default Layout

```text
structure/
  src/
  generated/
```

Default settings:

```toml
[tool.structure]
source_dir = "structure/src"
generated_dir = "structure/generated"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.2"

validate_inputs = true
validate_intermediate = true
validate_outputs = true

lineage = "basic"
streaming_compatibility_checks = true
strict_performance = true

format_generated = true
fail_on_diff = false
```

Generate a seed config:

```bash
structure init --seed-config
```

## pyproject.toml

Preferred user configuration:

```toml
[tool.structure]
# Only override values that differ from defaults.
generated_dir = "build/structure/generated"
fail_on_diff = true
```

## structure.toml

Alternative standalone file:

```toml
source_dir = "structure/src"
generated_dir = "structure/generated"
lineage = "fields"
```

## Config Resolution Order

1. CLI flags.
2. `pyproject.toml` `[tool.structure]`.
3. `structure.toml`.
4. seed defaults.

## Settings

### `source_dir`

Directory containing Structure schemas and transform source files.

Default:

```toml
source_dir = "structure/src"
```

### `generated_dir`

Directory where generated PySpark code is written.

Default:

```toml
generated_dir = "structure/generated"
```

IDE note: this directory should be a Python package if generated code is imported directly. Include `__init__.py` files or use namespace package behavior consistently across the project.

### `target_backend`

Currently supported:

```toml
target_backend = "pyspark"
```

### `target_pyspark`

Target PySpark version range.

```toml
target_pyspark = ">=3.5,<4.2"
```

The PySpark emitter uses this setting to choose compatible generated code patterns.

### Validation

```toml
validate_inputs = true
validate_intermediate = true
validate_outputs = true
```

Intermediate validation is enabled by default because subtransforms have typed return schemas.

### Lineage

```toml
lineage = "basic"
```

Supported levels:

```text
none
basic
fields
debug
```

Default: `basic`.

Basic lineage emits compact LDJSON events for transforms, inputs, steps, joins, hooks, and outputs.

### Streaming Compatibility

```toml
streaming_compatibility_checks = true
```

Structure v1 and v2 do not generate streaming orchestration. Generated transforms should be usable with streaming DataFrames when their operations are Spark streaming-compatible.

### Strict Performance

```toml
strict_performance = true
```

Strict performance mode rejects or warns about operations that would compromise Spark optimizer visibility.

Compiled subtransforms never silently fall back to UDFs.

### Generated Formatting

```toml
format_generated = true
```

Formats generated Python code after writing.

### Fail on Diff

```toml
fail_on_diff = true
```

Useful in CI to ensure generated code is committed and up to date.
