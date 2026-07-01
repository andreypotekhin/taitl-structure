# CLI

## Purpose

The Structure CLI is the local development and CI entrypoint for compiler work. It lets developers initialize
configuration, validate Structure source, generate optional PySpark artifacts, verify checked-in generated output,
inspect compiler understanding, and clean generated files.

The CLI is intentionally a compiler surface, not a Spark job runner. `structure check`, `structure compile`, and
`structure compile --fail-on-diff` must run without PySpark, Java, Spark startup, a `SparkSession`, or a Spark cluster.
Online runtime execution remains available through `StructureSession`, not through the v1 CLI.

## Command Surface

The v1 command set is:

```bash
structure init
structure init --seed-config
structure check
structure check --compat-targets TARGETS
structure check --profile
structure compile
structure compile --profile
structure compile --fail-on-diff
structure explain orders.transforms.order.EnrichOrders
structure clean
```

The Poetry entrypoint is already declared as:

```toml
[tool.poetry.scripts]
structure = 'structure.app.cli.api:cli'
```

The implementation should use Click, because the project already depends on `click`. The public entrypoint should be
`structure.app.cli.api:cli`, and the CLI app should delegate real work through `CliApp` to focused command and logic
classes under `structure/app/cli/`.

## Common Behavior

All commands should:

- run from the current working directory by default;
- accept an optional project directory flag when implementation adds project-root selection;
- resolve configuration before command-specific work, except `init`;
- print human-readable output by default;
- avoid stack traces for expected user errors;
- print unexpected internal failures as concise bug diagnostics;
- return process exit codes consistently.

Exit codes:

```text
0  success
1  expected Structure error, such as compileability, configuration, stale generated output, or invalid source
2  CLI usage error, such as an unknown option or missing argument
3  unexpected internal error
```

Output should be deterministic for the same source tree, configuration, Structure version, terminal width class, and
command flags. Diagnostics and profile summaries may include elapsed times, but generated code and diff comparisons
must not depend on wall-clock values.

## Configuration Resolution

Commands that compile, check, explain, or clean generated artifacts use this precedence:

1. CLI flags.
2. `pyproject.toml` `[tool.structure]`.
3. `structure.toml`.
4. defaults.

The resolved configuration must be visible to compiler phases as a single immutable config object. Unknown keys and
invalid values are errors. Configuration diagnostics must include the setting path, invalid value, allowed values or
expected type when known, suggested fix, and a link to [Configuration.md](../Configuration.md) or the relevant specification.

Recommended initial CLI override flags:

```text
--source-root PATH              repeatable; overrides source_roots
--generated-dir PATH            overrides generated_dir
--generated-package NAME        overrides generated_package
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

Implementations may stage these flags across milestones, but every supported flag must behave as a config override and
must appear in help text with the corresponding config key.

V1 accepts `--target-profile` and `--compat-targets` as reserved alternative-backend metadata. Non-PySpark compatibility
targets must be reported as pending, not as executed compatibility checks.

## `structure init`

`structure init` creates a Structure configuration file for a project.

Default behavior:

- if `pyproject.toml` exists, add `[tool.structure]` only when the table is absent;
- otherwise create `structure.toml`;
- write a compact recommended configuration;
- refuse to overwrite existing Structure configuration unless a future explicit force flag is added;
- print the file path written and a short next command, usually `structure check`.

`structure init --seed-config` writes every default setting with comments when comments are practical for the target
file format. This supports the user story "all default settings are visible."

Seed config should include at least:

```toml
[tool.structure]
source_roots = ["src"]
generated_dir = "generated"
generated_package = "structure_generated"
execution_mode = "online"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
hook_target_default = ["pyspark"]
traceability = "compiler"
validate_inputs = true
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
validate_outputs = true
output_validation_mode = "schema_only"
strict_performance = true
fail_on_diff = false
```

If no source root exists yet, `init` should still write valid configuration. It should warn that `src/` was not found
and suggest creating source packages or adjusting `source_roots`.

## `structure check`

`structure check` validates Structure source without writing generated files.

It must run:

1. configuration resolution and validation;
2. source-root resolution;
3. discovery and source inspection;
4. symbolic execution;
5. IR construction;
6. compileability checks;
7. compatibility checks for the configured target backend and PySpark range;
8. compiler provenance and static dataflow traceability construction in memory when enabled.

It must not write generated schemas, transforms, runtime support, provenance files, traceability files, cache files, or temp
artifacts that survive command completion. Temporary files are allowed only if they are cleaned before exit.

Successful output should be compact:

```text
Structure check passed
  source roots: src
  transforms: 3
  schemas: 8
  warnings: 0
```

Warnings do not fail the command by default. Errors fail with exit code `1`.

V1 `--compat-targets` behavior:

```bash
structure check --compat-targets pyspark,polars,duckdb
```

V1 validates the active PySpark target as usual, then prints a pending-status summary for listed non-PySpark targets.
It must not claim that Polars, DuckDB, Spark SQL, or Ibis checks have run. Future versions will replace the pending
summary with capability-engine portability reports. Unsupported active-target requirements remain errors.

## `structure compile`

`structure compile` validates source and writes generated artifacts.

It performs the same compiler pipeline as `check`, then writes:

- generated Spark schema declarations;
- generated PySpark transform classes;
- generated runtime support needed by generated code;
- compiler provenance files when traceability is enabled;
- static dataflow traceability files when traceability is enabled.

Generation must be deterministic. For unchanged content, the command should use write-if-changed behavior so file
timestamps do not churn and editor/build tools do not see false changes.

Successful output should summarize the work:

```text
Structure compile passed
  generated dir: generated
  transforms: 3
  files written: 12
  files unchanged: 28
  warnings: 0
```

`compile` must fail before writing partial generated output when configuration or source validation fails. Once writing
begins, it should use a safe strategy: write to temporary files and replace final paths atomically where practical, or
write each file only after its content is complete.

## `structure compile --fail-on-diff`

`--fail-on-diff` verifies that checked-in generated output is fresh.

Behavior:

1. run the compile pipeline;
2. generate all artifacts into a temporary directory using the same relative layout as `generated_dir`;
3. compare that temporary output with the configured generated directory;
4. fail with exit code `1` if any file is added, removed, or changed;
5. delete the temporary directory before exit.

This mode must not modify the configured generated directory.

Failure output must name changed paths and show enough context for CI users to act:

```text
CompileError GEN-E0901: Generated output is stale

Changed files:
  modified generated/structure_generated/orders/pyspark/transforms/order.py
  added    generated/structure_generated/runtime/schema_assert.py

Use:
  Run `structure compile` and commit the generated changes.

See docs/specifications/CLI.md
```

The comparison should normalize line endings so Windows checkouts do not fail solely because of CRLF/LF differences.
It must not normalize whitespace inside files otherwise.

## `structure explain`

`structure explain TRANSFORM` shows what the compiler sees for one transform.

`TRANSFORM` is a fully qualified transform class name, such as:

```bash
structure explain orders.transforms.order.EnrichOrders
```

The command must run configuration resolution, discovery, symbolic execution, IR construction, and compileability
checks for the requested transform and its dependencies. It must not write generated output.

Default output is a compact, stable text report:

```text
EnrichOrders
  module: orders.transforms.order

  inputs:
    orders: OrderRaw
    customers: Customer
    products: Product

  steps:
    normalize: OrderRaw -> OrderNormalized
      filters: 3
      hooks: after remove_negative_totals
      validates output: yes

    add_customer: OrderNormalized -> OrderWithCustomer
      joins:
        customers#1: LEFT join_one on customers.id == order.customer_id
      validates output: yes

  output:
    OrderEnriched
```

The report should include warnings relevant to the transform, such as unproven `join_one(...)` uniqueness. It should
identify opaque hook boundaries so a reader can distinguish compiled logic from arbitrary PySpark hooks.

Future compatibility explain output should show the active target and any requested compatibility targets. Hook
boundaries should include their effective target set and whether that target set was explicit or inherited from
configuration.

If the transform is not found, the diagnostic must show the requested name, the source roots searched, and a suggested
fix such as checking `source_roots` or the class name.

## `structure clean`

`structure clean` removes generated artifacts owned by Structure.

By default it removes only paths under the configured `generated_dir` that Structure can identify as generated output.
It must never delete files outside the project root or outside `generated_dir`. The implementation should use one or
both of these safety markers:

- a generated manifest written by `structure compile`;
- file headers that identify Structure-generated files.

If the configured `generated_dir` contains unknown files, `clean` should refuse to remove the directory wholesale and
print the unknown paths. A future explicit force flag may remove unknown files, but v1 should be conservative.

Successful output:

```text
Structure clean passed
  removed files: 12
  removed dirs: 5
```

## `--profile`

`--profile` reports compile-time performance metrics for `check` and `compile`.

Metrics:

- config load time;
- discovery time;
- source inspection time;
- symbolic execution time;
- IR construction time;
- checking time;
- code generation time;
- formatting time;
- compiler provenance time;
- static dataflow traceability time;
- total time;
- files considered;
- files written;
- transforms compiled;
- cache hits or cache hit ratio.

v1 profiling measures cold compiler work. Production incremental compilation belongs to v2, but v1 should preserve
source fingerprints, deterministic outputs, and stable phase boundaries so the later cache can reuse this structure.

Profile output should be compact and human-readable:

```text
Profile
  config: 4 ms
  discovery: 18 ms
  symbolic execution: 31 ms
  checking: 7 ms
  codegen: 22 ms
  formatting: 11 ms
  provenance: 3 ms
  traceability: 5 ms
  total: 101 ms
  files written: 12
  cache hits: 0
```

When used with `--fail-on-diff`, `files written` means files written to the temporary comparison directory.

## Diagnostics

Diagnostic code format, severity names, lifecycle rules, registry requirements, and stable documentation anchors are
owned by [Diagnostics.md](Diagnostics.md). This section defines CLI rendering and command-specific context.

CLI diagnostics should wrap compiler, configuration, discovery, generation, diff, and clean failures in one consistent
shape:

```text
CompileError BACKEND-E2402: Unsupported backend capability

Setting:
  target_pyspark = "<3.0"

Problem:
  Structure v1 supports PySpark 3.5.x and 4.0.x by default.

Use:
  Set `target_pyspark = ">=3.5,<4.1"` or choose a supported range.

See docs/specifications/BackendCapabilities.md
```

Diagnostics should include, when relevant:

- diagnostic code;
- command name;
- config file path and setting;
- source file and line;
- transform class;
- subtransform method;
- input, field, hook, join, or generated path;
- problem;
- suggested fix;
- documentation link.

The CLI should print warning diagnostics before the success summary. Error diagnostics should be printed without a
success summary.

## Spark-Free Compiler Contract

The CLI compiler commands are part of the no-Spark contract:

```text
structure check
structure compile
structure compile --fail-on-diff
structure explain
```

These commands must not import PySpark, start Java, create a `SparkSession`, connect to a Spark cluster, or require
Spark environment variables. They may import user source modules only if discovery keeps module import Structure-safe.
If a user module starts Spark at import time and discovery executes that import, Structure should fail with a diagnostic
that explains module imports must be compiler-safe and suggests moving Spark startup into runtime code.

## Implementation Interfaces

The CLI should be a thin shell over application logic. A practical decomposition is:

- `CliApp`: owns CLI feature instances;
- `CheckCommand`: runs config, discovery, symbolic execution, IR construction, and checks;
- `CompileCommand`: runs check pipeline, then writes generated artifacts;
- `ExplainCommand`: renders one transform report from compiler IR;
- `InitCommand`: creates configuration files;
- `CleanCommand`: removes generated artifacts safely;
- `ProfileReport`: phase timings and counters;
- `CompareGeneratedOutput`: compares temp generation with checked-in output;
- `RenderDiagnostic`: maps structured diagnostics to terminal text.

Command classes should expose a named-argument `__call__` entrypoint and delegate to stateless logic classes, matching
the repository's app structure guidance.

The compiler pipeline should expose reusable programmatic APIs so tests and future build integrations do not need to
shell out for ordinary behavior. The shell command remains the public interface for users and CI.

## Acceptance Criteria

The CLI implementation is complete when tests prove:

- `structure --help` lists all v1 commands;
- `structure init` writes compact config without overwriting existing config;
- `structure init --seed-config` writes all default settings;
- CLI flags override `[tool.structure]`, `structure.toml`, and defaults;
- `[tool.structure]` wins over `structure.toml`;
- `structure.toml` wins over defaults;
- unknown config keys fail with a setting path, value, suggested fix, and docs link;
- `structure check` succeeds on a valid minimal transform project without writing generated files;
- `structure check` fails on invalid source with exit code `1` and structured diagnostics;
- `structure compile` writes schemas, transforms, runtime support, provenance, and static dataflow traceability;
- `structure compile` uses write-if-changed behavior for unchanged generated files;
- `structure compile --fail-on-diff` passes when checked-in generated output matches compiler output;
- `structure compile --fail-on-diff` fails without modifying generated output when files differ;
- diff failure output lists added, removed, and modified generated paths;
- `structure explain` renders inputs, steps, filters, joins, hooks, validation, output schema, and warnings;
- `structure explain` fails clearly when the requested transform is not discovered;
- `structure clean` removes only Structure-owned generated files under `generated_dir`;
- `structure clean` refuses to remove unknown files without an explicit future force mode;
- `--profile` prints all required metrics for `check` and `compile`;
- `check`, `compile`, `compile --fail-on-diff`, and `explain` run without PySpark, Java, Spark startup, a
  `SparkSession`, or a Spark cluster;
- expected user errors use exit code `1`;
- CLI usage errors use exit code `2`;
- unexpected internal errors use exit code `3`.

## Test Placement

CLI implementation tests belong under `tests/app/cli/...`. Specification-backed user stories from
[UserStories.md](UserStories.md) should have tests under `tests/user_stories/...`. Tests that directly back this specification
document belong under `tests/specifications/cli/...`.

Recommended test groups:

- help and command wiring;
- config resolution and CLI overrides;
- init file creation;
- check success and negative diagnostics;
- compile output and write-if-changed behavior;
- `--fail-on-diff` stale output detection;
- explain report rendering;
- clean safety;
- profile metrics;
- no-Spark compiler contract.

Compiler command tests should use tiny fixture projects and assert filesystem effects explicitly. PySpark runtime tests
must remain separate from CLI compiler tests because the compiler commands are Spark-free by contract.
