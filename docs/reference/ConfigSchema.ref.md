# Configuration Reference

Configuration controls source discovery, generated output, execution mode, target selection, validation, traceability,
and CI behavior. Use this page to choose a configuration source, set a key, or correct an invalid value.

The [Configuration Schema background](../background/ConfigSchema.back.md) explains resolution and security rules. The
[Configuration guide](../Configuration.md) gives a longer setup walkthrough.

## Configuration sources

Values are resolved from lowest to highest precedence:

1. built-in defaults;
2. `structure.toml`;
3. `[tool.structure]` in `pyproject.toml`;
4. CLI flags or Python API overrides.

Overlapping keys from a higher layer replace lower values. Lists replace rather than append. Unknown keys and invalid
values fail before discovery or execution.

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"

[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

The plugin table is the current target-selection form. Legacy root keys such as `target_backend`, `target_profile`,
`target_variant`, and `compat_targets` must not be mixed silently with plugin selection; use the equivalent plugin
settings named by the diagnostic.

## Minimal configuration

For a normal PySpark project, this is enough:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"

[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

When no file exists, Structure discovers `src/` when it contains importable packages; otherwise it falls back to the
project root. Explicit `source_roots` always wins.

## Project and generated paths

| Key | Type and default | Rules |
| --- | --- | --- |
| `source_roots` | list of strings; discovered | Non-empty, existing paths; not inside `generated_dir` |
| `generated_dir` | string; `generated` | Project-relative output root; may be created by `compile` |
| `generated_package` | dotted name; `structure_generated` | Valid name; cannot be `structure` or a source package |
| `generated_docs` | Boolean; `false` | Write generated documentation during `compile` |
| `generated_docs_dir` | relative string; `docs` | Must stay inside `generated_dir` |
| `generated_docs_formats` | list; `["markdown", "json"]` | Supported values are `markdown` and `json` |
| `generated_code_options` | list; `[]` | Opt into generated method, expression, hook, or UDF forms |
| `generated_code_hard_wrap` | integer; `120` | Generated source line width |
| `allow_stage_outputs` | Boolean; `true` | Return recursively composed stage outputs alongside final outputs |

```toml
[tool.structure]
source_roots = ["src", "shared"]
generated_dir = "generated"
generated_package = "structure_generated"
generated_docs = true
generated_docs_dir = "docs"
generated_docs_formats = ["markdown", "json"]
```

Do not place source roots under the generated directory. Generated files are compiler output and must not be edited by
hand.

## Plugin and target selection

| Key | Meaning |
| --- | --- |
| `plugin.default` | Fallback plugin target |
| `plugin.disabled_distributions` | Normalized Python distributions excluded from discovery |
| `plugin.<name>.*` | Immutable options owned by the selected plugin |
| `plugin.pyspark.profile` | Supported PySpark version range |
| `plugin.pyspark.variant` | `ordinary` or `spark-connect` |

Target selection can also be supplied by a transform decorator, workflow `target=`, `StructureSession(target=...)`, or
a CLI override. One composed pipeline resolves one target before plugin services run. Plugin options are not passed to
other plugins.

`ordinary` targets the in-process PySpark `SparkSession`, DataFrame, and Column APIs. `spark-connect` uses the public
Spark Connect-compatible DataFrame and Column surface and rejects classic-only APIs. It does not change the Structure
DSL or transform `run(...)` signature.

Select the target in the project configuration so every composed transform resolves the same plugin:

```toml
[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "spark-connect"
```

Use a session or transform override only when the workflow intentionally needs a different effective target.

## Execution and validation

| Key | Values | Effect |
| --- | --- | --- |
| `execution_mode` | `online`, `generated` | Direct runtime execution or generated-code execution |
| `validate_inputs` | Boolean | Validate declared input schemas |
| `input_validation_mode` | `schema_only`, `schema_and_constraints` | Input validation depth |
| `validate_intermediate` | Boolean | Validate step and lane schemas |
| `intermediate_validation_mode` | `schema_only`, `schema_and_constraints` | Intermediate validation depth |
| `output_validation_mode` | `schema_only`, `schema_and_constraints` | Final-output validation depth |
| `strict_performance` | Boolean | Reject unsupported opaque or row-wise fallbacks |
| `warn_on_udfs` | Boolean | Warn when an opted-in Python UDF is used |
| `warn_on_lineage_growth` | Boolean | Warn when repeated lazy relation reuse risks driver-side logical-plan growth |
| `allow_stream_to_batch` | Boolean | Allow an undeclared downstream stream-to-batch boundary |

```python
from structure import *
from structure.plugin.pyspark import *

config = StructureConfig.resolve(
    project_root=".",
    execution_mode="generated",
    validate_intermediate=True,
    intermediate_validation_mode="schema_only",
)
session = StructureSession(spark=spark, config=config)
```

`schema_only` checks shape without row scans: names, types, nested structure, and reliable nullability. Constraint
validation is a separate opt-in cost. Spark Connect may default intermediate assertions off unless explicitly enabled.

## Traceability and CI

| Key | Values | Effect |
| --- | --- | --- |
| `traceability` | `none`, `compiler` | Omit or write compiler provenance and dataflow artifacts |
| `fail_on_diff` | Boolean | Treat stale generated output as an error |

`structure compile --fail-on-diff` overrides `fail_on_diff` for that invocation and never modifies the configured
generated directory. It compares temporary generated output with checked-in output and names added, removed, and
changed paths.

```bash
structure check --profile
structure compile --fail-on-diff
```

The first command diagnoses source and capability problems; the second verifies that checked-in artifacts match the
current source and resolved configuration.

## Spark SQL assumptions

Structure records the SQL assumptions used by compile-time typing and generated runtime behavior:

```toml
[tool.structure]
spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"
```

Structure does not create or reconfigure a Spark session. The caller supplies the session and remains responsible for
runtime settings, reads, writes, and streaming lifecycle.

## Python overrides

Use one resolved config when settings need to be shared or inspected:

```python
config = StructureConfig.resolve(
    project_root=".",
    overrides={"plugin": {"pyspark": {"variant": "spark-connect"}}},
)
session = StructureSession(spark=spark, config=config)
```

Do not mix `config=...` with duplicate `project_root=...` or config override fields on the same session. Build the
config first so precedence and validation are visible.

## Common corrections

- If a configured source path is missing or empty, create it or correct `source_roots`.
- If generated output is rejected as stale, run `structure compile` and commit the resulting files, or use the diff
  check only to verify freshness.
- If a target range is unsupported, use the supported PySpark profile shown by the diagnostic.
- If a Spark Connect run uses a classic-only API, switch to a supported public API or the ordinary variant.
- If a config diagnostic includes a secret-bearing future value, remove the secret from configuration; diagnostics must
  not be used as a secret transport.

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

This keeps paths and target selection in the documented configuration shape instead of relying on legacy root-level
target keys.

## Generated-code options

`generated_code_options` changes source shape, not transform meaning:

| Option | Effect |
| --- | --- |
| `mirror_methods` | Give generated classes constructor-held inputs and source-step methods |
| `embed_exprs` | Render expression specials as generated helpers |
| `embed_hooks` | Copy eligible raw-hook bodies into generated source |
| `embed_udfs` | Copy eligible opted-in UDF bodies into generated source |

`embed_hooks` requires standalone hook bodies: local imports, parameters, builtins, `self.spark`, and `self.ctx` are
allowed; module globals, closures, `super()`, and other instance state are rejected. A transform using a Python UDF
needs `embed_udfs` as well when the generated module must be source-transform-free. These options do not affect online
execution.

`generated_code_hard_wrap` defaults to `120`; values below `80` are rejected because nested PySpark expressions need
room to remain readable. Generated documentation can be Markdown for people, JSON for publishing and CI, or both.

```toml
[tool.structure]
generated_code_options = ["mirror_methods", "embed_exprs"]
generated_docs = true
generated_docs_formats = ["markdown", "json"]
generated_code_hard_wrap = 120
```

Keep `embed_hooks` and `embed_udfs` opt-in: they move caller-authored Python into generated source and therefore add
standalone-body and review requirements.

## Target and plugin details

Each plugin uses only the options under its own configuration table. Settings for an unselected plugin do not change
the active process. CLI `--target`, `StructureSession(target=...)`, capability resolution, and schema tooling use the
same target name.

The target profile is a version range, not a request to inspect the locally installed backend during compiler commands.
An unsupported range or operation fails capability checking before generated output or runtime execution.

| Key | Default | Meaning |
| --- | --- | --- |
| `compat_targets` | `[]` | Additional targets reported as pending compatibility metadata |
| `hook_target_default` | `["pyspark"]` | Effective target set for hooks without an explicit target |

Non-PySpark compatibility targets are not claimed as executed checks. `hook_target_default = "explicit"` requires every
hook to declare its target; `"configured"` limits unmarked hooks to the active target; `"all"` makes an opaque
portability claim and may produce warnings. Runtime execution refuses to call a hook outside its effective target set.

```toml
[tool.structure.plugin]
default = "pyspark"

[tool.structure]
hook_target_default = "explicit"
compat_targets = ["spark-connect"]
```

With this setting, every raw hook must state its target rather than inheriting a portability claim implicitly.

## Validation depth

The three validation phases are independent:

| Phase | Setting | Typical checks |
| --- | --- | --- |
| Input | `input_validation_mode` | Incoming names, types, nested shape, and nullability |
| Intermediate | `intermediate_validation_mode` | Step and lane contracts |
| Output | `output_validation_mode` | Published result schemas |

Allowed modes are `off`, `schema_only`, and `schema_and_constraints`. `validate_intermediate` remains a compatibility
shortcut; prefer the phase-specific mode in new configuration. Spark Connect may default intermediate assertions off
when omitted, while input and final-output validation remain strict. Set the intermediate mode explicitly when remote
plan-boundary diagnostics matter.

Shape validation is different from data quality. Accepted values, ranges, patterns, uniqueness, referential checks,
freshness, and row-count policies belong to explicit constraints and may trigger Spark work. Do not assume
`schema_only` validates row values.

```toml
[tool.structure]
input_validation_mode = "schema_only"
intermediate_validation_mode = "schema_and_constraints"
output_validation_mode = "schema_only"
```

This checks input and final shape cheaply while opting into richer checks only at intermediate boundaries where the
workflow accepts the additional runtime cost.

`spark.sql.ansi.enabled` and `spark.sql.storeAssignmentPolicy` document assumptions used by compile-time nullability
and type-coercion checks. They do not override the caller's existing Spark configuration. If the runtime session uses
different settings, treat assignment and error behavior as a compatibility risk.

## Effective configuration and diagnostics

The resolver produces one immutable effective configuration. It records the project root, source roots, generated
paths, execution mode, selected target, plugin options, validation, traceability, performance, diff, SQL assumptions,
and a source map identifying the default, file, or CLI flag that supplied each value.

Unknown keys and invalid values fail before discovery:

```text
CompileError CONF-E0101: Unknown configuration key

Setting:
  [tool.structure].generatedDirectory

Use:
  generated_dir = "generated"
```

```text
CompileError CONF-E0102: Invalid configuration value

Setting:
  [tool.structure].traceability = "fieldz"

Allowed:
  none, compiler, columns, debug
```

Diagnostics should identify the setting path, supplied value when safe, allowed values or expected type, corrective
action, and the narrowest documentation link. Secret-bearing settings are not part of the public configuration model;
future secret values must be redacted.

## Recommended configuration

- Choose one configuration source for shared project settings and rely on documented precedence.
- Keep source roots outside the generated directory and import-safe.
- Select one plugin target and profile for a composed pipeline.
- Use `schema_only` when shape checks are sufficient; opt into constraints deliberately.
- Treat generated output as compiler-owned and verify freshness with `--fail-on-diff` in CI.
- Keep `strict_performance = true` unless an explicit UDF or hook boundary is intended.
- Record SQL assumptions without expecting Structure to mutate the Spark session.

## Key-by-key quick reference

| Area | Keys |
| --- | --- |
| Source and output | `source_roots`, `generated_dir`, `generated_package` |
| Generated docs | `generated_docs`, `generated_docs_dir`, `generated_docs_formats` |
| Generated source | `generated_code_options`, `generated_code_hard_wrap` |
| Runtime | `execution_mode`, `plugin.default`, `plugin.<name>.*` |
| Validation | Input, intermediate, and output validation settings |
| Performance | `strict_performance`, `warn_on_udfs`, `allow_stream_to_batch` |
| Compatibility | `plugin.pyspark.profile`, `plugin.pyspark.variant`, `compat_targets` |
| Hooks | `hook_target_default` |
| Provenance and CI | `traceability`, `fail_on_diff` |
| SQL assumptions | `spark.sql.ansi.enabled`, `spark.sql.storeAssignmentPolicy` |

### Precedence examples

If `structure.toml` sets `execution_mode = "online"` and a CLI invocation supplies
`--execution-mode generated`, that invocation uses generated execution while the file remains unchanged. If both
`structure.toml` and `[tool.structure]` set `source_roots`, the `pyproject.toml` value wins. Python overrides follow
the same rule:

```python
config = StructureConfig.resolve(
    project_root=".",
    overrides={"spark.sql.ansi.enabled": False},
)
```

Higher-precedence plugin tables merge by key within the same plugin. Lists replace rather than append. A plugin option
that is not selected remains inert and is not passed to another plugin.

### Project layout

Use a separate source root and generated root so generated modules cannot be mistaken for authored source.

```text
project/
  pyproject.toml
  src/
    orders/
      schemas/
      transforms/
  generated/
    structure_generated/
      orders/
      runtime/
```

Each source root is an import root. Generated modules mirror source import paths below `generated_package`. Do not name
a user package `structure` unless shadowing the installed library is deliberate. Mark source roots and, when needed,
the generated root in the IDE rather than changing Python import behavior in source modules.

### Configuration failure order

Configuration should fail early, before source discovery, when:

1. a file cannot be parsed;
2. an unknown key is present;
3. a value has the wrong type or allowed-value spelling;
4. a path is empty, missing, nested under generated output, or otherwise unsafe;
5. plugin selection or profile is unsupported;
6. a combination such as legacy target keys plus plugin tables is ambiguous.

The correction should name the setting path and show the nearest valid value. A runtime Spark failure is too late for a
configuration error that could have been identified by `structure check`.

```bash
structure check --profile
```

Run the Spark-free check after each configuration change so path, key, target, and profile errors fail before runtime.

## Safe defaults

The recommended baseline is:

```toml
[tool.structure]
execution_mode = "online"
strict_performance = true
warn_on_udfs = true
validate_inputs = true
validate_intermediate = true
input_validation_mode = "schema_only"
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
fail_on_diff = false
```

Use generated mode, richer validation, traceability, or embedded bodies only when the workflow needs the corresponding
artifact or evidence. Keep a project-wide target profile stable while comparing generated output; changing the profile
is a semantic compatibility change, not only a formatting change.

## Configuration migration

When moving from root-level target settings to plugin selection, replace rather than mix the shapes:

```toml
[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

Do not retain legacy `target_backend`, `target_profile`, `target_variant`, or `compat_targets` keys beside plugin
tables unless the active release explicitly documents a compatibility bridge. Ambiguous configuration should fail with
a migration diagnostic instead of silently selecting a different plugin.

After changing a target, validation mode, generated option, or SQL assumption, rerun `structure check` before comparing
generated output so the effective configuration is part of the review.

The resolved configuration is the contract to inspect when a command behaves differently from a local default.

For incident reports, include the resolved mode, target, source roots, generated directory, validation settings, and
configuration fingerprint. That is usually enough to distinguish a source defect from a stale artifact or profile
mismatch without exposing credentials or other secret-bearing values.

## See also

- [CLI reference](CLI.ref.md)
- [Execution reference](Execution.ref.md)
- [Configuration guide](../Configuration.md)
- [Configuration Schema background](../background/ConfigSchema.back.md)
