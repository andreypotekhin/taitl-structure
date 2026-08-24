# Configuration

Structure works by convention and supports a small TOML configuration for project-wide settings.

Use configuration for paths, package names, execution mode, validation defaults, Spark SQL assumptions, compiler
traceability settings, performance policy, build behavior, and selected-plugin options.

## Defaults

All defaults live in `pyproject.seed.toml`. Most projects only need to specify settings that differ.

## pyproject.toml

Preferred:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
generated_docs = false
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
execution_mode = "online"
```

## structure.toml

Alternative:

```toml
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
generated_docs = false
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
execution_mode = "online"
```

## Python API

Runtime code can resolve the same effective configuration without editing TOML:

```python
from structure import *

config = StructureConfig.resolve(
    project_root=".",
    execution_mode="generated",
    generated_package="orders_generated",
)

session = StructureSession(spark=spark, config=config)
```

`StructureConfig.resolve(...)` starts with built-in defaults, merges `structure.toml`, merges
`pyproject.toml [tool.structure]`, and then applies keyword or mapping overrides. Keyword overrides work for ordinary
Python identifiers such as `execution_mode`; use `overrides={...}` for dotted keys such as
`"spark.sql.ansi.enabled"`.

## Plugin Options

Plugins may define their own project options. `plugin.default` selects the default target, while the selected plugin
owns the meaning of its own table. For example, PySpark owns `profile` and `variant`:

```toml
[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

Structure treats plugin tables as opaque: it merges and freezes them,
then sends only the selected plugin's table to that plugin's authoring, schema, and compiler facets. Structure neither
recognizes nor validates the option names or values.

For a one-command selection override, use `structure check --target pyspark` (or another installed plugin name).
It selects the command's target without changing project configuration.

Python callers can make the equivalent session-local choice with
`StructureSession(target="pyspark", runtime=...)`.

Core capability checks use the same target name: `Capabilities.resolve()(target="pyspark", options={...})`. The
selected plugin owns the option names and supplies the corresponding capability report.

Schema tooling follows the same convention: `StructureTools.schemas.generate(..., target="pyspark")` selects the
plugin that reads and renders the schema source.

```toml
[tool.structure.plugin.pyspark]
vendor_mode = "fast"
retry_policy = { attempts = 3, backoff = "linear" }
```

Use the same nested shape from Python:

```python
config = StructureConfig.resolve(
    project_root=".",
    overrides={"plugin": {"pyspark": {"vendor_mode": "fast"}}},
)
```

Higher-precedence configuration layers merge keys only within the same plugin table. Plugin tables are immutable in the
resolved `StructureConfig`; options for unselected plugins are never passed to the active plugin.

For one-off runtime settings, pass the common config keys directly to `StructureSession`:

```python
session = StructureSession(
    spark=spark,
    project_root=".",
    execution_mode="generated",
    generated_package="orders_generated",
)
```

Do not mix `config=...` with `project_root=...` or config override fields on the same session. Build the config first
when the settings need to be shared or inspected.

## Path Settings

```toml
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
generated_docs = false
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
```

`source_roots` is an ordered list of filesystem import roots. Each root contains importable Python packages or
modules. `generated_dir` is the generated-code filesystem root.

`generated_package` is the Python package below `generated_dir` that owns generated Structure artifacts.
`generated_docs` controls whether `structure compile` writes generated documentation artifacts.
`generated_docs_dir` is the generated documentation directory inside `generated_dir`; `generated_docs_formats`
controls whether enabled docs are written as Markdown, JSON, or both.

Recommended layout:

```text
src/orders/...
generated/structure_generated/store/...
```

Generated modules mirror source import paths below `generated_package`. For example, source module
`src/orders/transforms/order.py` generates below `generated/structure_generated/store/...`.

If no configuration is present, Structure resolves source roots by convention:

1. If `./src` exists and contains importable packages or modules, use `["src"]`.
2. Otherwise, use `["."]`.

Explicit configuration always wins.

IDE guidance:

- Mark every configured `source_roots` entry as a source root.
- Mark `generated` as a source root if you want generated-code navigation.
- Do not create a project package named `structure` unless you intend to shadow the installed Structure
  library.

## Validation-related Settings

```toml
validate_inputs = true
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
validate_outputs = true
output_validation_mode = "schema_only"
```

Intermediate validation is enabled by default because step method return types define intermediate schemas.
Set `validate_intermediate = false` to disable intermediate schema validation for generated step method
boundaries.

Spark Connect applies a variant-specific default: when `plugin.pyspark.variant = "spark-connect"` and
`validate_intermediate` is not explicitly set, intermediate schema assertions are disabled to avoid a remote
analysis request for every step. Input and final-output validation remain strict. Set `validate_intermediate = true`
to restore exhaustive intermediate checks. The separate `connect_plan_boundaries` option controls logical-plan
containment only:

```toml
[tool.structure.plugin.pyspark]
variant = "spark-connect"
connect_plan_boundaries = "auto"  # off, auto, or strict

# Optional exhaustive diagnostic mode.
validate_intermediate = true
```

`auto` inserts bounded temporary-view boundaries at branch points and at a conservative step cadence. `strict`
inserts one after every non-final step and is intended for diagnostics. Structure drops only the temporary views it
created; call `StructureSession.close()` (or `GeneratedTransform.close()`) when lazy results no longer need them.
Closing Structure resources never stops the caller-owned Spark session.

`input_validation_mode`, `intermediate_validation_mode`, and `output_validation_mode` control the cost and
depth of enabled validation at each phase:

```text
schema_only
schema_and_constraints
```

Default: `schema_only`.

`schema_only` validates schema shape only: column names, data types, nullable flags where Spark exposes them
reliably, nested struct shape, and missing or extra columns. It must not trigger row scans.

`schema_and_constraints` may add row-level constraint checks when Structure supports them. Use it deliberately
on pipelines where the additional Spark work is worth the stronger runtime contract.

Data-quality constraints are separate from schema shape. Accepted values, ranges, regex-like string checks,
decimal domain rules, uniqueness, referential checks, freshness, and row-count policies belong to an opt-in
constraint model. Any check that can trigger Spark actions must be explicit in source or configuration and
should link diagnostics to [DataQualityConstraints.md](reference/Schema.ref.md).

Future constraints should also bind to validation phases: input, intermediate, output, or a narrower named
boundary. The phase mode is a project-level cost guard. A constraint runs only when it is bound to the current
phase and that phase's validation mode allows constraints.

## Spark SQL Settings

```toml
spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"
```

Structure records Spark SQL assumptions with Spark's own dotted key names. These settings guide compile-time
nullability and type-coercion checks and document what generated runtime code expects from the caller's Spark
session.

Structure does not create or reconfigure Spark sessions.

## Execution and Target Settings

```toml
execution_mode = "online"

[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

`execution_mode` selects the runtime implementation. The stable default value, `online`, selects execution through
`StructureSession` and compiler IR. `generated` selects generated-code execution through checked-in generated PySpark
classes.

Allowed values:

```text
online
generated
```

`plugin.default` selects the installed target plugin. `plugin.pyspark.profile` constrains which PySpark APIs execution
and generated-code execution may use. The default targets
PySpark 3.5.x and 4.0.x. If a DSL feature cannot be generated for the configured profile, `structure check` and
`structure compile` should fail with `BACKEND-E2402` and name the unsupported capability. Unknown backend
targets fail with `BACKEND-E2401`. Backend capability behavior is specified in
[Capabilities background](background/Capabilities.back.md).

`plugin.pyspark.variant` selects the runtime variant inside the PySpark target. `ordinary` is the default in-process PySpark
contract. `spark-connect` supports completed compiler-visible batch features; streaming remains caller-owned ordinary
PySpark work. The variant does not change DSL syntax, generated class APIs, or `run(...)` signatures. See
[Compatibility.md](Compatibility.md).

## Generated Code Options

```toml
generated_code_options = ["mirror_methods", "embed_exprs"]
```

`generated_code_options` is an optional list of independent generated-source choices. Supported values are
`mirror_methods`, `embed_exprs`, `embed_hooks`, and `embed_udfs`. `mirror_methods` gives generated classes
constructor-held DataFrames and a zero-argument `run()` whose named methods mirror source schema steps. Expression
specials expand inline by default; `embed_exprs` renders them as generated helpers. `embed_hooks` and `embed_udfs`
embed the corresponding opted-in source bodies. An omitted list preserves the existing generated class layout and
source-backed hook/UDF delegation.

`embed_hooks` changes generated source only, never online execution. It copies each raw hook after generated `run(...)`
without its `@raw` decorator and removes the source-transform import and `_impl` field when no Python UDF needs them.
An embedded hook must be standalone: use local imports for runtime dependencies, parameters and local values for data,
and only `self.spark` or `self.ctx` from the generated instance. Source globals, closure values, `super()`, and other
instance state are rejected with `GEN-E0903`. A transform that uses a Python UDF must omit `embed_hooks` or also select
`embed_udfs`; the latter is required before the generated module can be source-transform-free.

`generated_code_hard_wrap` controls the maximum generated Python line length. The default is `120`; values below `80`
are rejected.

## Traceability Settings

```toml
traceability = "none"
```

Supported traceability levels:

```text
none
compiler
columns
debug
```

Default: `none`.

`compiler` records source-to-IR-to-generated provenance and compact static dataflow dependencies. `columns`
adds field-level static dependencies where the compiler can infer them. `debug` may include fuller expression
trees and source locations for troubleshooting.

## Performance Policy

```toml
strict_performance = true
warn_on_udfs = true
warn_on_lineage_growth = true
allow_pandas_udf = false
allow_rdd = false
allow_collect = false
allow_to_pandas = false
```

Compiled step methods never silently fall back to UDFs. These settings are primarily for hook linting and
future advanced features.

## Streaming Composition

```toml
allow_stream_to_batch = false
stream_to_batch_policy = "default"
```

`stream_to_batch_policy` is `"default"` or `"strict"`. Under `"default"`, Structure propagates effective streaming
lineage across an undeclared boundary and accepts it when the downstream compiler-visible steps are compatible.
Opaque boundaries remain an error. Under `"strict"`, an undeclared boundary is rejected unless global or local
`allow_stream_to_batch = true` opts into it. `allow_stream_to_batch` only bypasses the declaration guard; it never
suppresses a known streaming-incompatible operation. An explicit `input(..., streaming=False)` or
`@transform(streaming=False)` always fails at a streaming boundary.

## Compile-Time Performance

```toml
incremental_compile = false
compiler_cache_dir = ".structure/cache"
parallel_codegen = true
```

Structure should be fast enough for local development and CI.

Production incremental compilation is future work with no assigned version. The initial release may record source fingerprints and
avoid rewriting unchanged files, but it should not expose cache semantics that users must reason about.

## Build Settings

```toml
format_generated = true
fail_on_diff = false
```

Use `fail_on_diff = true` in CI to ensure generated code is committed and up to date.
