# Configuration Schema

Structure configuration controls source discovery, generated output, execution mode, target backend, validation,
traceability, Spark SQL assumptions, and CI behavior. Invalid configuration fails early with a structured diagnostic
that names allowed values.

This page covers configuration files, resolution order, keys, defaults, validation rules, diagnostics, and tests.
The normative sources are [ConfigSchema](../dev/specifications/ConfigSchema.spec.md) and
[PluginConfiguration](../dev/specifications/PluginConfiguration.spec.md).

## Plugin Configuration

Plugin selection replaces the root-level `target_backend`, `target_profile`, `target_variant`, and `compat_targets`
configuration
with plugin selection and plugin-owned option tables. Core validates selection syntax and passes the selected plugin
only its own immutable option mapping; a plugin owns the meaning of its option keys.

```toml
[tool.structure]
execution_mode = "online"

[tool.structure.plugin]
default = "pyspark"
disabled_distributions = []

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.2"
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

The normative selection contract is [PluginConfiguration.md](../dev/specifications/PluginConfiguration.spec.md).

The defaults and key descriptions below use the released plugin configuration. Legacy root-level target keys are
rejected with a diagnostic naming the equivalent plugin setting.

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
generated_docs = false
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
generated_code_options = []
generated_code_hard_wrap = 120
execution_mode = "online"
hook_target_default = ["pyspark"]
traceability = "none"
validate_intermediate = true
input_validation_mode = "schema_only"
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
strict_performance = true
warn_on_udfs = true
allow_stream_to_batch = false
fail_on_diff = false

spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"

[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.2"
variant = "ordinary"
runtime_version_check = "raise"
```

When no configuration file exists, source-root discovery may replace `source_roots = ["src"]` with `["."]` according
to [Transform](Transform.back.md#source-modules-and-expression-boundaries).

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

- Must be a project-relative directory path.
- The compiler may create it during `structure compile`.
- `structure check` must not require it to exist.

### generated_package

Type: string.

Default: `"structure_generated"`.

Rules:

- Must be a valid dotted Python package name.
- Must not be `"structure"`.
- Must not collide with a discovered source package.

### generated_code_options

Type: list of unique, non-empty strings. Default: `[]`.

Values are canonicalized into sorted order before entering `StructureConfig` and compiler options. The setting changes
generated source only; it does not change online semantics. `embed_hooks` copies eligible raw-hook methods into the
generated class when they use only local dependencies, parameters, Python builtins, and `self.spark` or `self.ctx`.
`embed_hooks` without `embed_udfs` is invalid for a transform using a Python UDF because the generated class would still
require the source implementation.

### generated_code_hard_wrap

Type: integer. Default: `120`. Generated Python should not contain lines longer than this value. Values below `80` are
rejected because deeply nested PySpark expressions need room to remain readable.

### generated_docs

Type: boolean.

Default: `false`.

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

### plugin.default

Type: non-empty plugin name. Default: `"pyspark"`.

Selects an installed plugin when neither a transform nor the workflow selects a target. Core passes the selected
plugin its own immutable option table. Removed root keys `target_backend`, `target_profile`, `target_variant`, and
`compat_targets` are rejected with migration guidance; they are not alternative configuration spellings.

### plugin.pyspark.profile

Type: supported profile string. Default: `">=3.5,<4.2"`.

The default admits the shared PySpark 3.5/4.0/4.1 feature set. Supported selections are `>=3.5,<4.1`,
`>=3.5,<4.2`, `>=3.5,<4.0`, `>=4.0,<4.1`, `>=4.1,<4.2`, and the separately gated future profile `>=4.2,<4.3`.
Arbitrary ranges are not accepted. Exact-4.1 APIs require `>=4.1,<4.2`; the installed runtime does not narrow a broad
profile automatically. Compiler commands resolve capabilities without reading installed PySpark or Spark versions.

### plugin.pyspark.variant

Type: string enum. Default: `"ordinary"`.

- `ordinary` uses the in-process PySpark SparkSession, DataFrame, and Column APIs.
- `spark-connect` uses public Connect-compatible DataFrame and Column APIs and rejects classic-only internals.

The variant does not change the Structure DSL, generated class API, or transform `run(...)` signature. Streaming
remains ordinary-PySpark-only; feature admission and optional provider dependencies follow the compatibility ledgers.

### plugin.pyspark.runtime_version_check

Type: string enum. Default: `"raise"`.

At execution time, the PySpark plugin compares the installed client and supplied engine versions with its configured
profile. `raise` reports a mismatch with `PYSPARK-E2710`, or unavailable version information with `PYSPARK-E2711`;
`log` records the diagnostic at ERROR and continues; `off` skips discovery. Static workflows do not run the check.
These policies do not alter feature admission or configure Spark. Generated code retains the selected profile,
variant, and policy and must be regenerated after changes.

### hook_target_default

Type: list of strings or string enum.

Default: `["pyspark"]`.

Status: recognized by configuration and hook decorators. The executable hook target remains PySpark.

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

Default: `"none"`.

`compiler` includes compiler provenance and static dataflow basics. `columns` and `debug` may be richer modes, but
must remain deterministic and documented before release.

### validate_intermediate

Type: boolean.

Default: `true`.

For the Spark Connect variant, an omitted value resolves to `false` through the PySpark semantic defaults. An explicit
value always wins.

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
- Documentation examples should keep this true.

### warn_on_udfs

Type: boolean.

Default: `true`.

Rules:

- When true, compiled transforms that use `@special(type="udf")` emit a warning because Python UDF bodies are opaque
  to Spark optimization.
- When false, UDFs still compile, but the optimizer-opacity warning is suppressed.

### allow_stream_to_batch

Type: boolean.

Default: `false`.

Rules:

- When false, a composed streaming output must be consumed by an input declared with `streaming=True`.
- When true, an undeclared downstream input may form an intentional stream-to-batch boundary.
- Explicit `input(..., streaming=False)` and `@transform(streaming=False)` remain compilation errors at a streaming
  boundary, regardless of this setting.

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

Detailed assignment rules are specified for `ANSI`.

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
  plugin
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

Configuration diagnostics must not print secrets. Structure config should avoid secret-bearing fields. If future
settings can include credentials or tokens, diagnostics must redact values by default.
