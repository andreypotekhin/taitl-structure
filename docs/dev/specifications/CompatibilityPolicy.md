# Compatibility Policy

The policy is summarized for users in [Compatibility.md](../../Compatibility.md). This specification defines the detailed
compatibility contract behind that page.

## Goals

The compatibility policy must:

- define supported Python versions;
- define supported PySpark versions and the default PySpark plugin profile range;
- define the boundary for future non-PySpark backends;
- define Spark Connect scope;
- define semantic versioning expectations;
- define direct runtime compatibility;
- define generated-code compatibility;
- define compiler traceability schema versioning;
- define config schema versioning.

## v1 Runtime Baseline

Structure v1 supports Python 3.11 and newer.

The default PySpark target is:

```toml
execution_mode = "online"

[plugin]
default = "pyspark"

[plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

This means execution and generated-code execution should target PySpark 3.5.x and 4.0.x APIs unless the user configures a
different target.

Airflow is not a hard dependency. Execution and generated-code execution should be usable from Airflow, Spark jobs, notebooks,
or other orchestrators without pulling in scheduler-specific runtime dependencies.

Linux is the v1 runtime target. Linux and macOS are the v1 development targets. Windows development may work where the
toolchain allows it, but Spark jobs should be designed and tested primarily for Linux deployment.

## PySpark Version Targeting

The PySpark target layer owns PySpark API compatibility. Discovery, symbolic execution, IR checks, traceability, and generic
diagnostics must not scatter PySpark-version conditionals unless a narrow check directly belongs there.

The target layer must be version-aware enough to:

- avoid APIs outside the configured PySpark plugin profile range;
- reject requested DSL features that cannot run for that range;
- produce diagnostics that state the required PySpark version when a feature is unavailable;
- keep online semantics and generated output deterministic for the same source, config, and Structure version.

Backend support checks are owned by [BackendCapabilities.md](BackendCapabilities.md). Compatibility checks must use that
interface instead of scattering PySpark-version or backend-feature conditionals across compiler phases.

When a target range spans multiple supported PySpark lines, Structure should prefer the oldest compatible API that keeps
the output clear and optimizer-visible.

## Alternative Backend Scope

Alternative backend support is specified in [AlternativeBackends.md](AlternativeBackends.md). The compatibility promise
applies to compiler-visible Structure source, not to hook bodies. Hooks are target-specific opaque runtime code and must
either declare `target_backend` or inherit a configured `hook_target_default`.

Future backend work is Python-hosted: v2-v4 prioritize PySpark-family targets such as Spark SQL and typed PySpark
DataFrame patterns. Polars LazyFrame, DuckDB, Ibis, and other non-PySpark backend expansion begin only after v4. Other
targets should come through Ibis when Ibis supports them. Dask DataFrame and Ray Dataset remain out of scope until after
the relational core is stable.
Unsupported active-target requirements must fail before execution or generation. Multi-target compatibility
checks may report non-active target issues as unsupported, degraded, opaque, or unknown.

## v5 Plugin Migration

M10 replaces the shared-source alternative-backend direction with one target platform per transform. This is a planned
breaking v5 boundary, not a reinterpretation of released configuration. The v5 Plugin API makes each target own its
DSL, schema extensions, semantic checks, lowering, and runtime values; Core retains workflow orchestration and public
artifact/diagnostic contracts. No v5 compatibility promise exists between transforms authored for different plugins.

At v5 release, `target_backend`, `target_profile`, and `target_variant` are removed. Their replacement is
`plugin.default`, `plugin.disabled_distributions`, and a target-owned `plugin.<name>` table, as specified in
[PluginConfiguration.md](PluginConfiguration.md). The bundled PySpark values move to
`plugin.pyspark.profile` and `plugin.pyspark.variant`. The v5 migration guide must include this mapping and must
fail legacy keys with an actionable configuration diagnostic rather than silently guessing how to translate them.

The plugin boundary also changes direct execution from `StructureSession(spark=...)` to
`StructureSession(runtime=..., context=..., config=...)`. The selected plugin validates the supplied runtime. Generated
artifacts are compatible only with their recorded plugin identity, plugin version, and negotiated Plugin API
version; users rebuild artifacts after any incompatible change.

## Spark Connect Scope

Spark Connect is a PySpark target variant, not a separate backend id. The configuration shape is:

```toml
[plugin]
default = "pyspark"

[plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "spark-connect"
```

Mainstream execution/generated-code execution targets ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs. Sprint
09 promotes Spark Connect from experimental parity to supported status for completed compiler-visible batch features
only, after live runtime evidence, diagnostics, and CI or documented verification are in place. V3 hardens streaming
transformations while callers retain lifecycle ownership.

Spark Connect support is intentionally narrow:

- it uses the existing PySpark target boundary cleanly;
- it does not change public DSL syntax;
- it does not change online invocation construction or `StructureSession` semantics;
- it does not change generated class construction or `run(...)` signatures;
- it does not change streaming orchestration semantics;
- it does not weaken generated-code readability or reviewability;
- it has parity evidence for completed compiler-visible batch features;
- public docs make the support level explicit.

Spark Connect must not rely on SparkContext, RDDs, direct JVM/Py4J access, `_jdf`, or private classic PySpark fields.
Unsupported variant capabilities must fail through backend capability diagnostics before execution or generated
code is claimed compatible. The detailed support contract is specified in [SparkConnect.md](SparkConnect.md).

## Semantic Versioning

After 1.0, Structure follows semantic versioning.

Major releases may:

- change public DSL behavior;
- change direct runtime API behavior;
- remove or change documented config keys;
- change generated-runtime helper contracts;
- change generated-code compatibility rules;
- drop supported Python or PySpark lines;
- make breaking compiler traceability or config schema changes.

Minor releases may:

- add DSL features;
- add config keys with defaults;
- add PySpark support;
- add diagnostics;
- improve generated code without changing semantics;
- improve execution without changing semantics;
- add compiler traceability fields in a backward-compatible way.

Patch releases may:

- fix bugs;
- refine diagnostics without changing outcomes;
- fix documentation;
- improve internal implementation without changing public behavior.

Before 1.0, minor releases may change public contracts, but every breaking change should include migration notes.

## Execution Compatibility

Execution is the default v1 runtime surface. Compatible execution means:

- transform invocations bind declared input DataFrames by name;
- `StructureSession` accepts caller-owned Spark sessions and optional hook context;
- execution preserves the same transform semantics as generated PySpark for supported v1 features;
- compiler commands remain Spark-free even though direct runtime execution may import PySpark.

Breaking changes to `StructureSession`, transform invocation binding, or execution/generated-code semantic parity require a
major version after 1.0 or a compatibility shim.

## Generated-Code Compatibility

Generated PySpark is optional committed build output owned by the Structure compiler.

The generator should include a compact header in generated files with:

- Structure generator version;
- configured backend;
- configured PySpark target range;
- configured target variant;
- source module or transform identity where useful.

Generated code may import Structure generated-runtime helpers. Those helpers are public to generated code, even if they
are not intended for direct end-user use.

Breaking generated-runtime helper changes require one of:

- a major Structure version;
- a compatibility shim;
- a regeneration strategy that makes old generated code fail with a clear upgrade diagnostic.

Upgrade guidance for projects that commit generated files must tell users to run:

```bash
structure compile --fail-on-diff
```

## Compiler Traceability Schema Versioning

Compiler traceability has two v1 metadata models:

- compiler provenance, which maps source nodes to IR nodes to generated PySpark nodes;
- static dataflow traceability, which records transform, table, and column dependencies inferred from IR.

The traceability schema version follows `major.minor`.

Breaking changes require a major traceability schema version bump. Additive fields require a minor version bump. Consumers
should ignore unknown fields so minor additions remain compatible.

Runtime LDJSON traceability is not part of the v1 compatibility contract. It remains future work beyond the published
v4 scope.

## Config Schema Versioning

Config schema versioning is implicit for v1. A future explicit key may expose it:

```toml
config_schema_version = 1
```

Unknown config keys and invalid values are errors. The diagnostic must include:

- the setting path;
- the invalid value;
- allowed values or expected type when known;
- a link to [Configuration.md](../../Configuration.md) or [Compatibility.md](../../Compatibility.md) when the problem is compatibility-related.

New optional keys may appear in minor releases. Removing or changing a documented key requires a major version after
1.0. Deprecated keys should warn before removal when practical.

## Acceptance Criteria

- [Compatibility.md](../../Compatibility.md) documents the public policy.
- `Readme.md` links to the compatibility policy.
- [Configuration.md](../../Configuration.md) documents plugin selection, PySpark plugin options, and compatibility
  diagnostics.
- [Configuration.md](../../Configuration.md) documents `execution_mode`.
- [BackendCapabilities.md](BackendCapabilities.md) documents the backend capability interface and PySpark v1 profile.
- [Roadmap.md](../Roadmap.md) and public roadmap text schedule Spark Connect batch support promotion for Sprint 09.
- The seed config defaults are `execution_mode = "online"`, `plugin.default = "pyspark"`,
  `plugin.pyspark.profile = ">=3.5,<4.1"`, and `plugin.pyspark.variant = "ordinary"`.
