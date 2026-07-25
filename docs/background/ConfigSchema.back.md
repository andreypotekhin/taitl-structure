# Configuration Schema

Structure configuration controls source discovery, generated output, execution mode, target backend, validation,
traceability, Spark SQL assumptions, and CI behavior. Configuration errors must fail early with structured diagnostics and
allowed values.

This reference covers configuration files, resolution order, keys, defaults, validation rules, diagnostics, and tests.

## Plugin Configuration

Plugin selection replaces the root-level `target_backend`, `target_profile`, `target_variant`, and `compat_targets` configuration
with plugin selection and plugin-owned option tables. Core validates selection syntax and passes the selected plugin
only its own immutable option mapping; a plugin owns the meaning of its option keys.

```toml
[tool.structure]
execution_mode = "online"

[tool.structure.plugin]
default = "pyspark"
disabled_distributions = []

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

`plugin.default` is the configured fallback target. A transform resolves one target from its decorator, an explicit
workflow `target=`, then this default. `plugin.disabled_distributions` prevents matching normalized Python
distribution identities from becoming eligible during metadata discovery. `plugin.plugin_options = "allow_injection"`
is the sole private-engine opt-in; it is absent by default and is not public plugin behavior.

A higher-precedence `plugin.<name>` table merges by key with the same lower-precedence table; lists replace rather
than append. Core does not require a configured plugin to be installed until a workflow selects it. The selected plugin
then receives only that table. PySpark owns `plugin.pyspark.profile` and `plugin.pyspark.variant`; Core treats both as
opaque plugin options.

CLI `--target`, `StructureSession(target=...)`, capability resolution, and schema tooling use the same generic target
name. A target selection never changes configuration files or creates process-wide active-plugin state. A composed
pipeline must resolve one identical target before a plugin service facet runs.

The normative selection contract is [PluginConfiguration.md](../dev/specifications/PluginConfiguration.md).

## Configuration Sources

Supported sources, lowest to highest precedence:

1. Built-in defaults.
2. `structure.toml`.
3. `[tool.structure]` in `pyproject.toml`.
4. CLI flags or Python API overrides.

If both `structure.toml` and `[tool.structure]` exist, `[tool.structure]` wins for overlapping keys. Non-overlapping
keys merge unless a later decision requires a single authoritative file.

CLI flags override both files for command-line workflows. Python API overrides supplied through
`StructureConfig.resolve(...)` or `StructureSession(...)` use the same validation rules and override both files for
runtime workflows.

## Default Configuration

Seed defaults:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
generated_docs = true
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
execution_mode = "online"
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "ordinary"
hook_target_default = ["pyspark"]
traceability = "compiler"
validate_intermediate = true
input_validation_mode = "schema_only"
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
strict_performance = true
warn_on_udfs = true
fail_on_diff = false

spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"
```

When no configuration file exists, source-root discovery may replace `source_roots = ["src"]` with `["."]` according
to [SourceModuleRules.md](SourceModuleRules.back.md)).

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

### generated_docs

Type: boolean.

Default: `true`.

Rules:

- `true` writes configured generated documentation artifacts during `structure compile`.
- `false` skips generated documentation artifacts and makes `compile --fail-on-diff` ignore existing files under
  `generated_docs_dir`.

### generated_docs_dir

Type: string.

Default: `"docs"`.

Rules:

- Must be a relative path inside `generated_dir`.
- Must not contain `..` path segments.
- The compiler may create it during `structure compile`.

### generated_docs_formats

Type: list of strings.

Default: `["markdown", "json"]`.

Allowed:

```text
markdown
json
```

Rules:

- The list must not be empty.
- `markdown` writes human-readable schema and transform reference pages.
- `json` writes the same public contract as machine-readable artifacts for CI or publishing tools.

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

### target_profile

Type: version range string.

Default: `">=3.5,<4.1"` in v1.

Rules:

- Must be parseable by the project's version range parser.
- Must resolve to a supported backend capability profile.
- For `target_backend = "pyspark"`, it selects the supported PySpark profile range.
- Must not inspect the locally installed backend version during compiler commands.

### target_variant

Type: string enum.

Allowed for `target_backend = "pyspark"`:

```text
ordinary
spark-connect
```

Default: `"ordinary"`.

Rules:

- `ordinary` targets the normal in-process PySpark `SparkSession`, `DataFrame`, and `Column` contract.
- `spark-connect` targets Spark Connect through the PySpark DataFrame and Column API while rejecting classic-only
  internals through backend capability diagnostics.
- Spark Connect must not require a different Structure DSL, generated class API, or transform `run(...)` signature.

### compat_targets

Type: list of strings.

Default: empty list.

V1 status: recognized and stored. `structure check` and `structure explain` may report non-PySpark targets as pending;
they must not claim Polars, DuckDB, or other future checks have run.

Future compatibility-report targets. This setting asks `structure check` and `StructureTools.compatibility` to report
whether compiler-visible Structure source is portable to additional backends. It does not change the active
`target_backend`.

### hook_target_default

Type: list of strings or string enum.

Default: `["pyspark"]`.

V1 status: recognized by configuration and hook decorators. The executable hook target remains PySpark.

Allowed future values:

```text
["pyspark"]
["configured"]
["all"]
explicit
```

Rules:

- The value supplies the effective target set for hooks that omit `target_backend`.
- `["pyspark"]` preserves existing PySpark hook behavior.
- `["configured"]` means unmarked hooks apply only to the active target.
- `["all"]` means the author claims hook ABI portability and should produce compatibility warnings because the hook body
  is opaque.
- `explicit` means every hook must declare `target_backend`.
- Runtime execution must refuse to call a hook outside its effective target set.

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

### warn_on_udfs

Type: boolean.

Default: `true`.

Rules:

- When true, compiled transforms that use `@special(type="udf")` emit a warning because Python UDF bodies are opaque
  to Spark optimization.
- When false, UDFs still compile, but the optimizer-opacity warning is suppressed.

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

See docs/background/CLI.back.md
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

See docs/background/CLI.back.md
```

## Effective Config

The resolver must produce an immutable effective configuration object:

```text
StructureConfig
  project_root
  source_roots
  generated_dir
  generated_package
  generated_docs
  generated_docs_dir
  generated_docs_formats
  execution_mode
  target_backend
  target_profile
  target_variant
  compat_targets
  hook_target_default
  traceability
  validation
  strict_performance
  warn_on_udfs
  fail_on_diff
  spark_sql
  source_map
```

`source_map` records which file, default, or CLI flag supplied each final setting for diagnostics and explain output.

## Security

Configuration diagnostics must not print secrets. v1 Structure config should avoid secret-bearing fields. If future
settings can include credentials or tokens, diagnostics must redact values by default.
