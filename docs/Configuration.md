# Configuration

Structure works by convention and supports a small TOML configuration for project-wide settings.

Use configuration for paths, package names, execution mode, validation defaults, Spark SQL assumptions, target
PySpark version, compiler traceability settings, performance policy, compatibility behavior, and build
behavior.

## Defaults

All defaults live in `pyproject.seed.toml`. Most projects only need to specify settings that differ.

## pyproject.toml

Preferred:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"
```

## structure.toml

Alternative:

```toml
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"
```

## Path Settings

```toml
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
```

`source_roots` is an ordered list of filesystem import roots. Each root contains importable Python packages or
modules. `generated_dir` is the generated-code filesystem root.

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
- Do not create a project package named `structure` unless you intend to shadow the installed Structure
  library.

## Validation Settings

```toml
validate_inputs = true
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
validate_outputs = true
output_validation_mode = "schema_only"
```

Intermediate validation is enabled by default because subtransform return types define intermediate schemas.
Set `validate_intermediate = false` to disable intermediate schema validation for generated subtransform
boundaries.

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
should link diagnostics to [DataQualityConstraints.md](specifications/DataQualityConstraints.md).

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

## Compatibility Settings

```toml
execution_mode = "online"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
```

`execution_mode` selects how transforms run. The default is `online`, where `StructureSession` executes
transforms at runtime from compiler IR. `generated` delegates runtime execution to checked-in generated
PySpark classes.

Allowed values:

```text
online
generated
```

`target_backend` selects the runtime backend. The initial release supports `pyspark`.

`target_pyspark` constrains which PySpark APIs online and generated execution may use. The default targets
PySpark 3.5.x and 4.0.x. If a DSL feature cannot be generated for the configured range, `structure check` and
`structure compile` should fail with `BACKEND-E2402` and name the unsupported capability. Unknown backend
targets fail with `BACKEND-E2401`. Backend capability behavior is specified in
[BackendCapabilities.md](specifications/BackendCapabilities.md).

Spark Connect is scheduled for v4 unless it can be added earlier without changing the public DSL, generated
class API, generated-code review model, or streaming orchestration contract. See
[Compatibility.md](Compatibility.md).

## Traceability Settings

```toml
traceability = "compiler"
```

Supported traceability levels:

```text
none
compiler
columns
debug
```

Default: `compiler`.

`compiler` records source-to-IR-to-generated provenance and compact static dataflow dependencies. `columns`
adds field-level static dependencies where the compiler can infer them. `debug` may include fuller expression
trees and source locations for troubleshooting.

## Performance Policy

```toml
strict_performance = true
allow_python_udf = false
allow_pandas_udf = false
allow_rdd = false
allow_collect = false
allow_to_pandas = false
```

Compiled subtransforms never silently fall back to UDFs. These settings are primarily for hook linting and
future advanced features.

## Compile-Time Performance

```toml
incremental_compile = false
compiler_cache_dir = ".structure/cache"
parallel_codegen = true
```

Structure should be fast enough for local development and CI.

Production incremental compilation is planned for v2. The initial release may record source fingerprints and
avoid rewriting unchanged files, but it should not expose cache semantics that users must reason about.

## Build Settings

```toml
format_generated = true
fail_on_diff = false
```

Use `fail_on_diff = true` in CI to ensure generated code is committed and up to date.
