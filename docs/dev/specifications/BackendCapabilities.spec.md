# Backend Capabilities

## Purpose

Backend capabilities describe what a configured execution target can run or generate from Structure IR. They answer
questions such as which PySpark versions are supported, which DSL features are available, whether streaming is safe,
which validation modes are allowed, and which imports generated code should use.

The capability contract keeps backend-specific rules out of discovery, symbolic execution, generic IR construction,
direct runtime orchestration, and generated-code text rendering. It is also the extension point for the alternative
backend contract specified in [AlternativeBackends.spec.md](AlternativeBackends.spec.md).

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

Feature specifications still own the domain meaning of a feature. For example, `JoinSemantics.spec.md` owns `lookup_join`
cardinality rules. This document owns whether the selected backend profile says `lookup_join` can be lowered.

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
  variant
  family
```

For the default PySpark profile, `name = "pyspark"`, `target = ">=3.5,<4.2"`, `variant = "ordinary"`, and
implementation `family = "ordinary_pyspark"`. For the Spark Connect variant, `name = "pyspark"`,
`target = ">=3.5,<4.2"`, `variant = "spark-connect"`, and semantic `family = "spark_connect_dataframe"`.
Future alternative-backend reports may add semantic-family vocabulary such as `pyspark_dataframe` or `sql_relation`;
that vocabulary must not require renaming the current v1 implementation family.

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
- `aggregate`;
- `compile`;
- `dedupe`;
- `docs`;
- `explain`;
- `higher_order`;
- `generator`;
- `relation`;
- `query`;
- `schema`;
- `set`;
- `pyspark`;
- `delta`;
- `iceberg`;
- `observation`;
- `geospatial`;
- `join`;
- `optimization`;
- `validation`;
- `streaming`;
- `window`;
- `imports`.

Future alternative backends may add these groups:

- `runtime`;
- `output`;
- `hook`;
- `type`.

`name` is the feature inside that group, such as `null_safe_equality`, `lookup_join`,
`schema_only_validation`, `stream_static_left_join`, or `generated_pyspark_imports`.

`source` is optional structured context such as transform, step, field, join, hook, or config setting.

`docs` points to this specification or a narrower semantic specification.

Capability requirements describe typed Structure operations. Shared requirements can cover common grouped,
higher-order, or join semantics; individual requirements such as `expression.to_time` distinguish operations whose
version or runtime support differs. The feature catalogs carry the exhaustive API inventory. Capability profiles
decide whether checked operations can run for a target without introducing an untyped function-dispatch fallback.
This keeps version choices in the target layer and applies equally to future plugins.

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

Supported decisions may leave diagnostic fields empty except for `docs`. Unsupported decisions provide the
feature-specific problem, rationale, remedy, documentation, and available target requirements.

## PySpark Profile

The default `>=3.5,<4.2` profile admits common PySpark 3.5/4.0/4.1 semantics. Profile strings are explicit keys,
not arbitrary version expressions. The supported keys and numeric bounds live together in
`PySparkCapabilities.py`; execution-time version checking uses those same bounds without changing static admission.
See the [profile reference](../../reference/ConfigSchema.ref.md#pyspark-profiles) for the supported selections.

Common requirements cover typed expressions, projections, filters, joins and strategy hints, grouped and window
metrics, higher-order collections, relation ordering/partitioning, set operations, schema validation, and generated
imports. Streaming support is further restricted by operation shape, target variant, state policy, and live evidence.
The [API tracker](../../compatibility/APITracker.md) and feature ledgers carry the operation inventory.

| Feature family | Admission rule |
| --- | --- |
| Column callbacks, five scalar additions, TIME, relation queries, observations, KLL/Theta, and Arrow UDFs | Exact `>=4.1,<4.2`; ordinary and Connect support is stated per operation and execution mode |
| Native Geometry/Geography schema types | Exact 4.0/4.1 profiles plus available runtime type constructors |
| Apache spatial helpers | Exact 4.1 ordinary/Connect batch; Spark feature flag and isolated provider session required |
| Databricks and Sedona helpers | Explicit namespaces and provider-specific requirements; managed Databricks live evidence remains open |
| Row state processors | Ordinary exact 4.1, with the typed state/timer/restart contract |
| Pandas state processors | Ordinary exact 4.0/4.1; legacy Pandas state keeps its independent evidence gate |
| Connect batch checkpointing | Exact 4.0/4.1 profiles; earlier Connect profiles reject it |
| Delta and Iceberg | Optional provider packages on the evidenced ordinary profiles and exact Connect 4.1; dependencies are checked separately |

Unimplemented or unproven requirements return unsupported decisions. No path silently falls back to Python UDFs,
row-wise execution, or opaque backend code. A future profile being recognized does not establish runtime support for
its gated operations. Static capability admission and execution-time runtime suitability are distinct checks.

## Alternative Backend Extension

Future backend profiles must be admitted through the rules in [AlternativeBackends.spec.md](AlternativeBackends.spec.md).

Minimum profile data:

```text
BackendId
  name
  target
  variant
  family

CapabilityProfile
  supported
  degraded
  unsupported
  modes
```

The active target treats unsupported and unknown required capabilities as errors. Multi-target compatibility checks may
also report degraded, opaque, and unknown features so users can decide whether a transform is portable enough for their
project.

Backends must not be supported by rewriting user Structure source. They must consume the same checked IR and either
lower it honestly or fail with a backend diagnostic before execution or generation.

## Diagnostics

Backend capability diagnostics use the component prefix defined in [Diagnostics.spec.md](Diagnostics.spec.md).

### BACKEND-E2401

Severity: error

Title: Unsupported backend target

Common cause: configuration or a workflow selects a plugin target with no available capability profile.

Use: select an installed target with `plugin.default` or workflow `target=`, such as the bundled `pyspark` plugin.

### BACKEND-E2402

Severity: error

Title: Unsupported backend capability

Common cause: source or IR asks the configured backend to lower a feature outside its supported capability profile.

Use: when a feature is supported on another admitted profile or variant, name the exact canonical plugin setting
that enables it. For an exact-4.1 feature under the broad default, show
`[tool.structure.plugin.pyspark] profile = ">=4.1,<4.2"` and the applicable variant. Do not describe an evidenced
feature as awaiting parity. Provider-specific or unimplemented features retain their dependency or design remedy.

Both diagnostics must include backend, target range, feature group, feature name, problem, rationale, suggested fix,
and documentation link. `BackendDiagnostic.to_diagnostic()` preserves its specific `why`, `use`, and `docs` rather
than replacing them with generic registry text. Direct exceptions and CLI rendering must expose the same remedy.

## PySpark Target Variants

`plugin.default = "pyspark"` selects the plugin; `plugin.pyspark.variant` selects a variant of that plugin.

```toml
[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.2"
variant = "ordinary"
```

The ordinary variant uses a normal in-process PySpark `SparkSession`, `DataFrame`, and `Column` contract. Its backend
family is `ordinary_pyspark`.

```toml
[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.2"
variant = "spark-connect"
```

The Spark Connect variant uses the PySpark DataFrame and Column API over a remote session. Its backend family is
`spark_connect_dataframe`. It accepts the completed compiler-visible batch feature families covered by the ordinary
PySpark profile: projections, filters, joins, analytical joins, aggregates, exact/subset dedupe, selected-row helpers,
window projections, rolling metrics, and higher-order array/map helpers.

Spark Connect rejects classic-only assumptions before execution or generation. These requirements are ordinary-only:
`backend.spark_context`, `backend.rdd_access`, `backend.jvm_access`, and `backend.private_classic_fields`.

## No-Spark Contract

Capability selection and checks must not import PySpark, start Java, create a `SparkSession`, connect to a Spark
cluster, or inspect the developer machine's installed Spark runtime. They use static source metadata selected from
configuration.

Runtime PySpark execution tests may import PySpark. Capability tests and compiler tests may not require it.

## Acceptance Criteria

- `plugin.default = "pyspark"` with `plugin.pyspark.profile = ">=3.5,<4.2"` and `variant = "ordinary"` resolves the
  ordinary PySpark capability profile.
- The PySpark plugin with `plugin.pyspark.variant = "spark-connect"` resolves the Spark Connect PySpark-variant
  capability profile.
- Spark Connect supports completed compiler-visible batch feature capabilities and rejects classic-only backend
  internals with `BACKEND-E2402`.
- Unknown selected targets fail with `BACKEND-E2401`.
- Unsupported feature requirements fail with `BACKEND-E2402`.
- Future backend profiles can report degraded, opaque, and unknown capabilities without importing backend runtimes.
- Supported v1 feature requirements return supported decisions.
- Generated import metadata is deterministic.
- Tests prove capability selection and checks do not import PySpark.
