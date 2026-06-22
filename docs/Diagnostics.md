# Diagnostics

Structure diagnostics are stable messages for user-correctable errors, warnings, and selected internal failures. They
are designed for humans first, but each diagnostic also has a machine-readable code that tests, CI annotations, IDEs,
and documentation links can target.

## Diagnostic Shape

Every published diagnostic must include:

- a stable code such as `CONF-E0101`;
- a severity: error, warning, info, or internal;
- a short title;
- the affected command, setting, source location, transform, field, hook, join, generated path, or runtime input when
  relevant;
- the problem in user-facing language;
- why it matters when the risk is not obvious;
- the shortest safe fix;
- a documentation link.

The CLI renders the same diagnostic model used by the compiler and runtime. Different renderers may change formatting,
but they must not change the code, severity, title, or documented meaning.

## Component Prefixes

Codes use this form:

```text
CONF-E0101
DSL-W0403
GEN-I0901
```

The prefix names the component that issued the diagnostic. The letter after the prefix is severity:

```text
E  error
W  warning
I  info
X  unexpected internal failure
```

Initial component prefixes are:

```text
CORE    diagnostic framework and internal fallback errors
CONF    configuration
DISC    source roots, discovery, imports, and package layout
SCHEMA  schema declaration, schema model, and schema inheritance
DSL     DSL, symbolic execution, expressions, filters, and expression helpers
IR      intermediate representation and generic compileability checks
JOIN    joins
HOOK    hooks
VAL     schema validation, validation placement, and data quality constraints
BACKEND backend capabilities and compatibility
STREAM  streaming compatibility
GEN     generated output, formatting, stale diffs, provenance, and traceability artifacts
ONLINE  online execution, sessions, transform invocation, and input binding
CLI     CLI command behavior, clean safety, profile output, and command usage
```

The four digits identify the diagnostic within that component. Reusing a removed code for a different meaning is never
allowed.

## Link Contract

Published diagnostics link to stable anchors in public documentation or specifications. The preferred public anchor is
the lowercase Markdown heading anchor for the code:

```text
docs/Diagnostics.md#conf-e0101
```

When a diagnostic needs deeper technical context, that entry may link onward to a specification such as
`docs/specifications/DSL.md` or `docs/specifications/JoinSemantics.md`.

Once a code appears in a release, its anchor must remain available. If the diagnostic is replaced, keep the old anchor
and point it to the replacement code.

## Registry Status

The implementation registry will be the source of truth for:

- code;
- severity;
- title;
- owner area;
- lifecycle status;
- public documentation anchor;
- suggested fix template;
- test fixture expectations.

Lifecycle statuses are:

```text
draft       reserved but not released
active      valid and tested
deprecated  still emitted, but a replacement exists
retired     no longer emitted, anchor kept for compatibility
```

The implementation registry in `structure.lib.cross.errors` is the source of truth for emitted diagnostic codes.
This document provides the stable public anchors those registry entries link to.

## Active Codes

### CONF-E0101

Severity: error

Status: active

Title: Unknown configuration key

Common cause: a project configuration file contains a `[tool.structure]` or `structure.toml` key that Structure does
not recognize.

Use: remove the key, correct its spelling, or move it to the documented configuration location.

### CONF-E0102

Severity: error

Status: active

Title: Invalid configuration value

Common cause: a known configuration key has a value of the wrong type or outside the allowed values.

Use: set the value to one of the allowed values shown in the diagnostic.

### DSL-E0401

Severity: error

Status: active

Title: Unsupported symbolic expression

Common cause: compiled transform code uses ordinary Python behavior that cannot be lowered to Spark-plan-visible
Column expressions.

Use: replace the expression with Structure DSL helpers, an `@expr_fn` helper, or an explicit hook when arbitrary
PySpark is the honest escape hatch.

### DSL-E0402

Severity: error

Status: active

Title: Invalid transform structure

Common cause: a transform class, subtransform signature, schema flow, or returned schema object does not match the
Structure compiler contract.

Use: check `@transform`, declared `input(...)` schemas, row parameter annotations, subtransform return annotations,
schema transition order, and assigned output fields.

### GEN-E0901

Severity: error

Status: active

Title: Generated output is stale

Common cause: `structure compile --fail-on-diff` found generated files that differ from current source and
configuration.

Use: run `structure compile`, review the generated diff, and commit the generated changes.

### GEN-E0902

Severity: error

Status: active

Title: Generated transform is not importable

Common cause: generated mode is selected, but the generated PySpark module or generated transform class cannot be
imported from Python's import path.

Use: run `structure compile`, ensure the generated source root is importable, or switch to
`execution_mode = "online"`.

### ONLINE-E1201

Severity: error

Status: active

Title: Transform input is missing

Common cause: runtime transform execution started before every declared `input(...)` had a bound DataFrame.

Use: pass every declared input DataFrame to the transform invocation before calling `run(session)`.

### ONLINE-E1202

Severity: error

Status: active

Title: Online PySpark runner is not configured

Common cause: online execution is selected in a session that does not yet have a live PySpark executor installed.

Use: pass an online executor to `StructureSession` or use `execution_mode = "generated"`.

### ONLINE-E1203

Severity: error

Status: active

Title: Execution mode is unsupported

Common cause: caller code supplied an execution mode other than `online` or `generated`.

Use: set `execution_mode = "online"` or `execution_mode = "generated"`.

### ONLINE-E1001

Severity: error

Status: draft

Title: Unknown transform input

Common cause: runtime transform construction received an input name that the transform did not declare with
`input(...)`.

Use: pass only the declared input names shown in the diagnostic.

### BACKEND-E2401

Severity: error

Status: active

Title: Unsupported backend target

Common cause: project configuration selects a `target_backend` for which Structure has no static capability profile.

Use: set `target_backend = "pyspark"` for v1.

### BACKEND-E2402

Severity: error

Status: active

Title: Unsupported backend capability

Common cause: source or IR asks the configured backend to lower a feature outside its supported capability profile.

Use: choose a supported Structure operation, use an explicit hook when arbitrary PySpark is the honest escape hatch, or
wait until the feature's specification and backend profile promote it.

### CLI-X1101

Severity: internal

Status: active

Title: Unexpected internal failure

Common cause: Structure hit a bug or an unclassified failure path.

Use: rerun with debug output when available and report the command, diagnostic code, Structure version, and concise
reproduction steps.

### STREAM-E0801

Severity: error

Status: active

Title: Transform is not streaming-compatible

Common cause: a transform marked or checked for streaming compatibility contains an operation that v1 classifies as
batch-only, such as an unsupported join shape or a future stateful operation.

Use: keep the transform batch-only, or rewrite the operation using v1 streaming-compatible projection, filtering,
schema-only validation, and stream-static left or inner joins.

### STREAM-W0801

Severity: warning

Status: active

Title: Hook streaming compatibility is unknown

Common cause: a transform has a hook without `streaming_safe=True`. Hooks are arbitrary PySpark code, so Structure
cannot prove they avoid actions, RDD/Pandas conversion, streaming lifecycle APIs, or stateful streaming operations.

Use: mark the hook `streaming_safe=True` only after verifying it satisfies the streaming contract, or keep the transform
batch-only.
