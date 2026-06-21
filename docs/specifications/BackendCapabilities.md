# Backend Capabilities

## Purpose

Backend capabilities define what a configured execution target can lower from Structure IR. The interface keeps
PySpark version choices, unsupported feature checks, streaming support, validation support, and generated import names
out of discovery, symbolic execution, generic IR construction, online runtime orchestration, and generated-code text
rendering.

This specification resolves C23 from `docs/dev/design/Challenges.md`.

## Scope

This specification owns:

- backend capability identity;
- capability requirement groups and names;
- capability decision shape;
- unsupported backend diagnostics;
- generated import-name capability;
- PySpark v1 default capability profile;
- Spark-free capability selection;
- tests for capability decisions.

Feature specifications still own the domain meaning of a feature. For example, `JoinSemantics.md` owns `join_one`
cardinality rules. This document owns whether the selected backend profile says `join_one` can be lowered.

## Interface

The implementation must expose an internal capability object:

```text
BackendCapabilities
  id
  imports()
  supports(requirement)
  require(requirement)
```

`id` identifies the backend:

```text
BackendId
  name
  target
  family
```

For v1, `name = "pyspark"`, `target = ">=3.5,<4.1"`, and `family = "ordinary_pyspark"` by default.

`imports()` returns deterministic generated import metadata for the backend. For PySpark v1 this includes the aliases
for `pyspark.sql.functions`, `pyspark.sql.types`, `DataFrame`, `SparkSession`, `Column`, and Structure generated
runtime schema helpers.

`supports(requirement)` returns a `CapabilityDecision` and must never raise.

`require(requirement)` returns a supported decision or fails through the backend diagnostic path.

## Requirement Shape

```text
CapabilityRequirement
  group
  name
  source
  docs
```

Allowed groups:

- `backend`;
- `expression`;
- `join`;
- `validation`;
- `streaming`;
- `imports`.

`name` is the feature inside that group, such as `null_safe_equality`, `join_one`,
`schema_only_validation`, `stream_static_left_join`, or `generated_pyspark_imports`.

`source` is optional structured context such as transform, step, field, join, hook, or config setting.

`docs` points to this specification or a narrower semantic specification.

## Decision Shape

```text
CapabilityDecision
  backend
  requirement
  supported
  code
  title
  problem
  why
  use
  docs
  required_target
```

Supported decisions may leave diagnostic fields empty except for `docs`. Unsupported decisions must fill every
diagnostic field.

## v1 PySpark Profile

The v1 PySpark capability profile supports these requirements:

```text
backend.ordinary_pyspark
expression.field_ref
expression.literal
expression.projection
expression.filter
expression.boolean_ops
expression.equality
expression.null_safe_equality
expression.cast
expression.standard_helper_call
join.join_one
join.left_join
join.inner_join
join.composite_equi_join
join.broadcast_hint
validation.schema_only_validation
validation.strict_projection
validation.allow_extra_projection
streaming.row_local_projection
streaming.row_local_filter
streaming.stream_static_left_join
streaming.stream_static_inner_join
imports.generated_pyspark_imports
```

Deferred features must be represented as unsupported decisions. Examples:

```text
join.join_many
join.exists
join.not_exists
join.lookup_dedupe
join.temporal_one
join.as_of_one
expression.aggregation
expression.window
streaming.stream_stream_join
streaming.streaming_orchestration
backend.spark_connect
```

Unsupported does not mean impossible forever. It means the feature is not part of the current backend contract and must
not be lowered through silent fallback, Python UDFs, row-wise operations, or backend-specific ad hoc code.

## Diagnostics

Backend capability diagnostics use the component prefix defined in `docs/specifications/Diagnostics.md`.

### BACKEND-E2401

Severity: error

Title: Unsupported backend target

Common cause: configuration selects a `target_backend` for which Structure has no capability profile.

Use: set `target_backend = "pyspark"` for v1.

### BACKEND-E2402

Severity: error

Title: Unsupported backend capability

Common cause: source or IR asks the configured backend to lower a feature outside its supported capability profile.

Use: choose a supported Structure operation, use an explicit hook when arbitrary PySpark is the honest escape hatch, or
wait until the feature's specification and backend profile promote it.

Both diagnostics must include backend, target range, feature group, feature name, problem, rationale, suggested fix,
and documentation link.

## No-Spark Contract

Capability selection and checks must not import PySpark, start Java, create a `SparkSession`, connect to a Spark
cluster, or inspect the developer machine's installed Spark runtime. They use static source metadata selected from
configuration.

Runtime PySpark execution tests may import PySpark. Capability tests and compiler tests may not require it.

## Acceptance Criteria

- `target_backend = "pyspark"` with `target_pyspark = ">=3.5,<4.1"` resolves a PySpark capability profile.
- Unknown `target_backend` fails with `BACKEND-E2401`.
- Unsupported feature requirements fail with `BACKEND-E2402`.
- Supported v1 feature requirements return supported decisions.
- Generated import metadata is deterministic.
- Tests prove capability selection and checks do not import PySpark.
