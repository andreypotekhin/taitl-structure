# Diagnostics

Structure diagnostics are the stable error and warning contract for configuration, discovery, schemas, symbolic
execution, IR validation, joins, hooks, backend capability checks, streaming compatibility, generated-code drift, CLI
behavior, and runtime execution.

Diagnostics must explain what failed, why it matters, and how to fix it. They are also stable enough for tests, CI
annotations, IDEs, troubleshooting guides, and documentation links.

## Scope

This reference covers:

- diagnostic code format;
- code-range ownership;
- severity names;
- required message fields;
- registry data model;
- documentation anchor stability;
- lifecycle rules;
- test expectations;
- renderer responsibilities.

Feature references still own the domain meaning of their diagnostics. For example,
[JoinSemantics.md](JoinSemantics.back.md)) owns which join shapes are invalid. This document owns the code, lifecycle, and
documentation contract that makes that join diagnostic stable.

## REL-E0701

`REL-E0701` means an `exactly_one(...)` relation assertion found a relation cardinality other than one at Spark
evaluation time. Structure checks this with Spark-visible DataFrame expressions, so generated and direct online
execution both fail during action evaluation without collecting relation rows to the driver.

Common causes:

- the asserted relation was not filtered to the intended policy row;
- the upstream input is empty;
- the upstream input contains duplicate policy/configuration rows;
- the transform used `exactly_one(...)` where ordinary row multiplication is valid.

Fix the source relation so it produces one row before the assertion, aggregate it to one row intentionally, or remove
the assertion and use an explicit join when multiple rows are intended.

## REL-E0702

`REL-E0702` means a `require_unique(...)` relation assertion found duplicate keys during Spark evaluation. Structure
checks duplicate groups in the Spark plan and fails through a Spark assertion expression; it does not collect the
relation to the driver.

Common causes:

- the declared key is incomplete;
- upstream data has duplicate catalog/configuration identifiers;
- deduplication or aggregation was expected but not declared.

Fix the source relation, declare a fuller business key, or explicitly deduplicate/aggregate before the assertion.

## REL-E0703

`REL-E0703` means a `require_all(...)` relation assertion found at least one row where the predicate is not true. Null
predicate results count as failures.

Common causes:

- an input catalog contains invalid ranges or required fields;
- the predicate is too strict for legitimate data;
- upstream normalization did not run before validation.

Fix or filter invalid rows before asserting `require_all(...)`, or loosen the predicate when those rows are valid.

## REL-E0704

`REL-E0704` means a `require_reference(...)` relation assertion found at least one checked value without a matching
row in the declared reference relation. By default null checked values are allowed; `nulls="reject"` treats nulls as
missing references.

Common causes:

- a catalog contains a parent or foreign-key value that is absent from the reference relation;
- upstream normalization uses different physical key values on the checked and reference sides;
- null values are invalid but were not rejected before validation.

Correct the referenced catalog, normalize both sides to the same business key, or filter invalid rows before asserting
`require_reference(...)`.

## REL-E0705

`REL-E0705` means a `select_first_qualified(...)` relation operation could not choose one deterministic candidate for
each declared key under its configured policies. With `missing="error"`, every key in the candidate relation must have
at least one eligible row. With `ties=TiePolicy.ERROR`, no key may have two eligible rows with the same priority value.

Common causes:

- the eligibility predicate filters out every candidate for a required key;
- the priority expression is not specific enough to break ties;
- the declared key is too broad and combines unrelated candidates into one partition;
- missing candidates are valid, but the operation was configured with `missing="error"`.

Fix the eligibility predicate, declare a more specific business key, add a deterministic priority tie-breaker in a
future supported priority expression, or use `missing="allow"` when absent candidates should simply produce no row.

## Design Principles

Diagnostics must be:

- actionable;
- deterministic for the same source, configuration, command, Structure version, and target capability set;
- structured before they are rendered;
- linked to stable documentation;
- testable by code and high-signal fields, not by whole terminal snapshots only;
- free of live Spark objects, Python object memory addresses, and nondeterministic values;
- concise by default, with enough context for a developer to act without reading source internals.

## Code Format

Published codes use:

```text
CONF-E0101
DSL-W0403
GEN-I0901
CLI-X1101
```

The component prefix names the subsystem that issued the diagnostic. The severity letter is:

```text
E  error
W  warning
I  info
X  unexpected internal failure
```

The four digits are stable within the component prefix. The severity letter may change only when the lifecycle rules
allow a compatible change. If a warning becomes an error in a way that can break CI, create a new error code and
deprecate the warning code.

## Component Prefixes

```text
CORE    diagnostic framework and internal fallback errors
CONF    configuration
DISC    source roots, discovery, imports, and package layout
SCHEMA  schema declaration, schema model, and schema inheritance
DSL     DSL, symbolic execution, expressions, filters, and expression helpers
IR      intermediate representation and generic compileability checks
JOIN    joins
REL     relation-level assertions and relation operations
HOOK    hooks
VAL     schema validation, validation placement, and data quality constraints
BACKEND backend capabilities and compatibility
CONNECT Spark Connect runtime and tooling compatibility boundaries
STREAM  streaming compatibility
GEN     generated output, formatting, stale diffs, provenance, and traceability artifacts
ONLINE  execution, sessions, transform invocation, and input binding
CLI     CLI command behavior, clean safety, profile output, and command usage
```

New feature references must either use an existing component prefix or reserve a new prefix here before publishing
examples with codes.

## Diagnostic Model

Every diagnostic is a structured value before rendering:

```text
Diagnostic
  code
  severity
  title
  message
  context
  problem
  why
  use
  docs
  source
  lifecycle
```

Required fields:

- `code`: stable machine-readable code.
- `severity`: error, warning, info, or internal.
- `title`: short human-readable summary.
- `problem`: what is wrong.
- `use`: the shortest safe fix or next action.
- `docs`: stable documentation link.

Required when available:

- `source`: source file, line, column, and expression display.
- `context`: command, config setting, transform, step method, input, field, hook, join, target backend, generated path,
  or runtime argument.
- `why`: why the problem matters when the risk is not obvious.

Diagnostic values may include additional structured fields for renderers and integrations. Unknown fields must not
change the meaning of the diagnostic code.

When an exact location is available, diagnostics use `primary_span` and optional `related_spans`. A span has a
project-relative display path, one-based Unicode character lines and columns, a short optional label, and a bounded
source excerpt captured while the diagnostic is built. `primary_span` identifies the text to change; each related span
identifies supporting source such as a conflicting declaration. Renderers show these spans inside the existing
`Source:` section. Diagnostics without a trustworthy span retain their logical `source` display and must never fail
merely because source text is unavailable.

## Registry

The implementation must contain a diagnostic registry before broad diagnostic work begins. The registry may be a Python
module, data file, or generated artifact, but it must be reviewed as source and must be available to tests without
importing PySpark.

Each registry entry must include:

```text
code
severity
title
owner
status
docs
introduced
problem_template
use_template
```

Optional entry fields:

```text
why_template
replaced_by
context_schema
examples
```

The registry must reject duplicate codes at test time. It should also reject missing docs links, malformed codes,
unknown statuses, and codes that use an unknown or wrong component prefix.

## Lifecycle

Registry status values are:

```text
draft       reserved but not released
active      valid and tested
deprecated  still emitted, but a replacement exists
retired     no longer emitted, anchor kept for compatibility
```

Rules:

- A draft code may change before release.
- An active code must not change meaning.
- A deprecated code must name `replaced_by`.
- A retired code must keep its documentation anchor.
- A code must never be reused for a different meaning.
- Public examples must not use draft codes unless the surrounding document says the example is provisional.

Compatible changes:

- rewording a message without changing meaning;
- adding context fields;
- improving a suggested fix;
- linking to a more specific documentation section while preserving the old anchor.

Potentially breaking changes:

- changing what condition emits a code;
- changing warning to error;
- removing a code from public docs;
- changing the documented fix to require a different user action.

Breaking changes require a new code unless the project is still before the first public release and the affected code
has not been published.

## Documentation Contract

[Diagnostics.md](../Diagnostics.md) is the compact public index. Every active, deprecated, and retired published code must have a
stable lowercase Markdown heading anchor there:

```text
docs/Diagnostics.md#conf-e0101
```

The public entry should contain:

- code and title;
- severity;
- short explanation;
- common causes;
- suggested fix;
- links to deeper documentation or references.

Specialized specs may contain richer examples, but diagnostics should link through the public index when practical so
external tools have one stable target.

## Rendering

Renderers map structured diagnostics to a surface:

- CLI terminal text;
- test assertion strings;
- CI annotations;
- future IDE diagnostics;
- runtime exceptions.

Renderers may choose layout, indentation, color, and truncation. They must preserve:

- code;
- severity;
- title;
- problem;
- suggested fix;
- docs link.

CLI rendering should avoid stack traces for expected Structure errors. Unexpected internal errors should use an
internal diagnostic such as `CLI-X1101`, include a concise bug-report prompt, and keep the underlying exception
available to logs or debug mode when that exists.

## Determinism and Security

Diagnostics must not include:

- memory addresses;
- object `repr(...)` output that embeds addresses;
- absolute workspace paths in deterministic artifacts;
- secrets from config values;
- full data samples from user DataFrames;
- live Spark application identifiers unless a runtime diagnostic explicitly needs them.

When a diagnostic includes a path, prefer project-relative paths. When it includes a config value that may be sensitive,
show the setting path and redact the value.
