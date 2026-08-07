# CLI Reference

The `structure` CLI checks and generates Structure source, explains compiler plans, initializes configuration, and
cleans Structure-owned artifacts. It is a compiler interface, not a Spark job runner.

The [CLI background](../background/CLI.back.md) explains command behavior and safety. Use the [Configuration
reference](ConfigSchema.ref.md) for keys and precedence.

## Commands

| Command | Use | Writes project output? |
| --- | --- | --- |
| `structure init` | Create a compact project configuration | Yes, configuration only |
| `structure init --seed-config` | Create a commented configuration with defaults | Yes, configuration only |
| `structure check` | Validate source and compatibility | No |
| `structure compile` | Validate and write generated artifacts | Yes |
| `structure compile --fail-on-diff` | Verify checked-in generated output is fresh | No |
| `structure explain CLASS` | Show compiler understanding of one transform | No |
| `structure clean` | Remove identifiable Structure-generated artifacts | Yes, generated artifacts only |

All commands use the current working directory as the project by default. Expected errors are rendered without a
stack trace. Exit code `0` means success, `1` means an expected Structure error, `2` means CLI usage error, and `3`
means an unexpected internal error.

## Initialize a project

```bash
structure init
structure init --seed-config
```

If `pyproject.toml` exists, `init` adds `[tool.structure]` only when that table is absent. Otherwise it creates
`structure.toml`. It refuses to overwrite existing Structure configuration. The command prints the file written and a
suggested next command, normally `structure check`.

`--seed-config` makes defaults visible, including source and generated paths, execution mode, target profile,
validation, traceability, performance, and diff settings. A missing `src/` directory is a warning, not a reason to
write invalid configuration.

## Check source

```bash
structure check
structure check --compat-targets pyspark,polars,duckdb
structure check --profile
```

`check` resolves configuration, discovers source modules, captures symbols, constructs the compiler plan, validates
compileability, checks the active target, and builds in-memory provenance when enabled. It does not write generated
schemas, transforms, runtime files, provenance, traceability, or persistent cache files.

Typical success output is compact:

```text
Structure check passed
  source roots: src
  transforms: 3
  schemas: 8
  warnings: 0
```

`--compat-targets` validates the active PySpark target and reports other named targets as pending metadata. It must not
claim that unsupported backends were executed.

## Compile artifacts

```bash
structure compile
structure compile --profile
```

Compile performs the same checks as `structure check`, then writes generated schema declarations, PySpark transform
classes, runtime support, and optional provenance or traceability files. Generation is deterministic and uses
write-if-changed behavior.

If validation fails, the command must fail before writing partial output. Generated files are owned by Structure:
change source or configuration and regenerate them rather than editing them directly.

## Verify generated output

```bash
structure compile --fail-on-diff
```

This mode generates into a temporary directory, compares it with the configured `generated_dir`, reports added,
removed, and changed files, then deletes the temporary directory. It does not modify the configured generated output.

When it fails, run `structure compile` and review/commit the generated changes. Line-ending differences alone are
normalized; whitespace inside files is not.

## Explain a transform

```bash
structure explain orders.transforms.order.EnrichOrders
```

Explain resolves and checks the requested fully qualified class without writing output. The report can include:

- source module and named inputs;
- source-order steps and input/output schemas;
- filters, joins, hooks, and validation boundaries;
- final outputs;
- relevant warnings, such as unproven lookup uniqueness;
- active target and hook target scope when configured.

If the class is not found, the diagnostic names the class, searched source roots, and a corrective action.

## Clean generated artifacts

```bash
structure clean
```

`clean` removes only files under the configured `generated_dir` that are identified by a Structure manifest or
generated-file header. It refuses to remove unknown files and does not delete outside the project root or generated
directory. Review unknown paths and remove them deliberately if they are not needed.

## Configuration overrides

Supported override names follow the configuration reference:

```text
--source-root PATH
--generated-dir PATH
--generated-package NAME
--execution-mode online|generated
--target-backend pyspark
--target-pyspark RANGE
--target-profile RANGE
--compat-targets TARGETS
--traceability none|compiler
--validate-intermediate / --no-validate-intermediate
--intermediate-validation-mode schema_only|full
--strict-performance / --no-strict-performance
```

Flags have higher precedence than `pyproject.toml`, `structure.toml`, and defaults. Every supported flag corresponds to
a resolved setting and appears in command help.

## Spark-free commands

The compiler commands below must not import PySpark, start Java, create a `SparkSession`, connect to Spark, or require
Spark environment variables:

```bash
structure check
structure compile
structure compile --fail-on-diff
structure explain orders.transforms.order.EnrichOrders
```

Imported user modules must also be compiler-safe. Move Spark startup, reads, writes, and runtime orchestration out of
module import time.

## Profile output

`--profile` reports phase timings and counts for `check` and `compile`, including configuration, discovery, source
inspection, symbolic execution, plan construction, checking, code generation, formatting, provenance, traceability,
total time, files considered/written, transforms, and cache hits when available. Elapsed times are diagnostic only and
must not affect generated content or diff results.

```bash
structure check --profile
structure compile --profile
```

Use profile output to find an expensive compiler phase; do not treat it as a Spark execution benchmark.

## Diagnostics

CLI diagnostics identify the code, setting or transform, problem, remedy, and relevant documentation. Common remedies
include:

```text
invalid source root       -> create the path or change source_roots
unsupported target        -> choose a supported plugin profile
stale generated output   -> run structure compile
unknown transform        -> check the fully qualified name and source_roots
unknown clean file       -> review the path before removing it
```

## Command behavior details

Configuration is resolved before command-specific work, except for `init`. Output is deterministic for the same source
tree, configuration, Structure version, terminal-width class, and flags. Timings may appear in profile output, but wall
clock values must not affect generated files or diff results.

`init` does not overwrite an existing Structure table or configuration file. To change an existing project, edit the
selected file or use a higher-precedence CLI/Python override. Keep secrets out of configuration because diagnostics and
generated artifacts are designed for review and publication.

The active target and compatibility targets are separate. A pending target is a portability request, not an execution
claim. If the active target is invalid, checking fails even when every pending target is informational.

```bash
structure check --target-backend pyspark --compat-targets spark-connect
```

This executes checks for the active PySpark target while reporting Spark Connect as compatibility metadata.

### Compilation and diff boundaries

The `compile` output includes generated schemas, transform modules, runtime support, manifests, and enabled provenance,
traceability, and documentation artifacts. It writes complete files with write-if-changed behavior and should not
leave partial output after a failed validation.

`--fail-on-diff` compares a temporary tree with the configured generated directory. It reports relative paths as added,
removed, or modified, then deletes the temporary tree on success or failure. The comparison covers every enabled
generated artifact; line-ending differences alone are normalized, but whitespace inside files is not.

```bash
structure compile --fail-on-diff
```

The command is safe for a pull-request check because it does not update the configured generated directory.

### Explain and clean boundaries

Explain is useful for checking inferred lane binding before execution. It is a plan explanation, not a runtime profile:
it does not load data, prove source uniqueness from rows, or start Spark. Warnings such as an unproven lookup
cardinality remain visible so a caller can add a dedupe or source constraint.

Cleaning is intentionally narrower than deleting the configured directory. Unknown files, user-authored files, and
paths outside the generated root remain untouched. If a generated directory is shared with another tool, configure a
dedicated Structure-owned root before using `clean`.

```bash
structure clean
```

Review the command's cleanup diagnostic before removing any unknown file that remains outside Structure's manifest.

## Configuration overrides in practice

Flags have higher precedence than `pyproject.toml`, `structure.toml`, and defaults. An override changes only that
invocation; it does not rewrite TOML configuration or update checked-in generated output.

```bash
structure check --source-root src --generated-dir build/structure
structure compile --execution-mode generated --generated-package orders_generated
structure explain orders.transforms.order.EnrichOrders --target-backend pyspark
```

Use the [Configuration reference](ConfigSchema.ref.md) for the complete key set. Use `StructureSession` for live
DataFrame execution; the CLI does not replace `readStream`, `writeStream`, query lifecycle, sinks, checkpoints, or
application orchestration.

## Command decision guide

| Need | Command |
| --- | --- |
| Create a first config | `structure init` |
| See every default | `structure init --seed-config` |
| Validate without writing | `structure check` |
| Write generated code | `structure compile` |
| Verify generated code in CI | `structure compile --fail-on-diff` |
| Inspect one transform's plan | `structure explain F.Q.Class` |
| Remove owned generated output | `structure clean` |

## Diagnostic correction patterns

| Diagnostic | First correction |
| --- | --- |
| Unknown config key | Use the exact key in the Configuration reference |
| Invalid source root | Create the path or update `source_roots` |
| Unsupported target capability | Choose a supported profile or operation |
| Compileability error | Read the named transform, step, and source anchor |
| Stale generated output | Run `structure compile` and review the diff |
| Unsafe clean path | Keep unknown files and configure a dedicated generated root |
| Missing transform | Check the fully qualified name and searched source roots |

Expected errors should include the diagnostic code, setting or transform, problem, remedy, and documentation link. The
CLI should not print a success summary after an error.

## Command contracts

### `structure check`

The check pipeline is:

```text
configuration
  -> source-root resolution
  -> discovery and inspection
  -> symbolic execution
  -> plan construction
  -> compileability and capability checks
  -> in-memory provenance and traceability
```

The command leaves no generated schema, transform, runtime, provenance, traceability, cache, or persistent temporary
file behind. Temporary work is allowed only when it is cleaned before exit.

### `structure compile`

Compile runs the same pipeline, then writes:

- generated Spark schema declarations;
- generated PySpark transform classes;
- runtime support required by those classes;
- manifests and file headers used by safe cleanup;
- optional provenance, traceability, and generated documentation.

Write-if-changed behavior prevents timestamps and editor watchers from changing when content is identical. A failed
configuration or source check must not leave partial generated output.

```bash
structure compile --generated-dir generated
```

### `structure explain`

Explain accepts a fully qualified transform class, such as
`orders.transforms.order.EnrichOrders`. Its compact report should show named inputs, source-order steps, filters,
joins, hooks, validation, outputs, active target, and warnings. It does not inspect live rows or prove uniqueness from
data; it explains the static compiler plan.

```bash
structure explain orders.transforms.order.EnrichOrders
```

### `structure clean`

Clean uses a Structure manifest or generated-file headers to identify files. It removes only identifiable output
under `generated_dir`. Unknown files make cleanup conservative rather than destructive. Do not treat clean as a general
project-directory deletion command.

```bash
structure clean --generated-dir generated
```

## CI patterns

Use a read-only check for pull requests that should not rewrite the worktree:

```bash
structure check --profile
structure compile --fail-on-diff
```

Use ordinary compile in a generation job that intentionally updates artifacts:

```bash
structure compile
git diff -- generated/
```

Keep the project configuration and generated package importable in CI. If a generated class is not found at runtime,
the CLI remedy is to compile, expose the generated source root, or choose online execution; it is not to silently run a
different source version.

## No-Spark troubleshooting

If a compiler command imports PySpark or starts Java, inspect imported user modules first. Common causes are a global
`SparkSession` construction, a module-level DataFrame read, a service client initialized at import time, or a streaming
query started from a module body. Move those actions to the caller or a raw hook and keep the module declaration-only.

If a project requires Spark just to discover source, the source layout violates the compiler contract. `structure
check` must remain usable in a build environment that has Python and Structure but no Java, PySpark runtime, or Spark
cluster.

```python
# Imported source should declare types and transforms only.
class Orders(Transform):
    orders = input(Order)

# Create Spark sessions and read DataFrames in the caller, not at module import time.
```

## Exit-status guide

| Code | Meaning | Typical action |
| ---: | --- | --- |
| `0` | Command completed successfully | Continue the workflow |
| `1` | Expected Structure failure | Read the diagnostic and correct source/config/output |
| `2` | CLI usage failure | Correct the option, argument, or command spelling |
| `3` | Unexpected internal failure | Preserve the diagnostic and report a bug |

Warnings do not fail `check` or `compile` by default, but they appear before a success summary. An error suppresses the
success summary so scripts cannot mistake a failed command for a completed generation.

## What the CLI controls

| Concern | CLI | Caller/application |
| --- | --- | --- |
| Source discovery and compileability | Controls | Provides import-safe source |
| Generated artifact writing | Controls during `compile` | Reviews and commits when desired |
| Spark session and job lifecycle | Does not control | Controls |
| Streaming source, sink, checkpoint | Does not control | Controls |
| Storage and deployment | Does not control | Controls |

This separation keeps the CLI useful in build systems while leaving runtime orchestration in the Python application.

## Reproducible command practice

For a reproducible build, pin the Structure version and target profile, run from the project root, keep source roots
ordered, and avoid machine-specific absolute paths in configuration. Use `--profile` to compare compiler phases, not to
compare Spark job latency. Review generated diffs together with the source and configuration change that caused them.

For a local diagnosis, start with `structure check`, inspect `structure explain F.Q.Class`, then run `structure compile`
only after the plan and warnings are understood. Use `--fail-on-diff` as a final freshness gate rather than as the
primary diagnostic command because it deliberately avoids changing the configured generated directory.

In automation, capture both the exit code and the diagnostic text. Do not parse success counts as proof of Spark
execution: compiler commands are intentionally Spark-free. For runtime execution use the Python API and the execution
reference.

For release automation, keep `check` and `--fail-on-diff` as separate gates: the first diagnoses source and capability
errors, while the second verifies artifact freshness. Both remain safe to run without a Spark installation.

```bash
structure check
structure compile --fail-on-diff
```

## CI artifact policy

Treat generated files as reproducible build artifacts:

1. Run `structure check` against the same source roots and profile used for compilation.
2. Run `structure compile` in a clean generated directory when the artifact is part of the release.
3. Run `structure compile --fail-on-diff` against the checked-in directory when freshness is required.
4. Review the generated diff together with the source, configuration, and Structure version.
5. Fail the job when diagnostics, artifact drift, or import validation reports an error.

Keep the compiler command deterministic by fixing the project root and avoiding environment-dependent source order.
Do not make a CI pass depend on a developer's local generated cache. If the generated directory is disposable, clean it
before compilation; if it is checked in, use the diff gate to prove that it matches the current source.

The CLI does not decide whether generated code should be committed. That repository policy belongs to the project, but
the command output should make the selected directory, artifact changes, and failure boundary unambiguous.

```bash
structure compile --generated-dir build/structure
git diff -- build/structure
```

When a command is reproduced outside the original checkout, pass the same project root and explicit profile rather than
assuming the current working directory supplies them. This keeps diagnostics and generated paths stable across local,
CI, and release environments.

Prefer machine-readable exit status and captured diagnostics over scraping incidental progress text from a terminal.

## See also

- [Configuration reference](ConfigSchema.ref.md)
- [Transform reference](Transform.ref.md)
- [Execution reference](Execution.ref.md)
- [CLI background](../background/CLI.back.md)
