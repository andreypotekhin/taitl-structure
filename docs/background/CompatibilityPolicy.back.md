# Compatibility Policy

The policy is summarized for users in [Compatibility.md](../Compatibility.md). This reference defines the detailed
compatibility contract behind that page.

## Goals

The compatibility policy must:

- define supported Python versions;
- define supported PySpark versions and the default `target_profile` range;
- define the boundary for future non-PySpark backends;
- define Spark Connect scope;
- define semantic versioning expectations;
- define online runtime compatibility;
- define generated-code compatibility;
- define compiler traceability schema versioning;
- define config schema versioning.

## v1 Runtime Baseline

Structure v1 supports Python 3.11 and newer.

The default PySpark target is:

```toml
execution_mode = "online"
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "ordinary"
```

This means online and generated execution should target PySpark 3.5.x and 4.0.x APIs unless the user configures a
different target.

Airflow is not a hard dependency. Online and generated transforms should be usable from Airflow, Spark jobs, notebooks,
or other orchestrators without pulling in scheduler-specific runtime dependencies.

Linux is the v1 runtime target. Linux and macOS are the v1 development targets. Windows development may work where the
toolchain allows it, but Spark jobs should be designed and tested primarily for Linux deployment.

## PySpark Version Targeting

The PySpark target layer owns PySpark API compatibility. Discovery, symbolic execution, IR checks, traceability, and generic
diagnostics must not scatter PySpark-version conditionals unless a narrow check directly belongs there.

The target layer must be version-aware enough to:

- avoid APIs outside the configured `target_profile` range;
- reject requested DSL features that cannot run for that range;
- produce diagnostics that state the required PySpark version when a feature is unavailable;
- keep online semantics and generated output deterministic for the same source, config, and Structure version.

Backend support checks are owned by [BackendCapabilities.md](BackendCapabilities.back.md)). Compatibility checks must use that
interface instead of scattering PySpark-version or backend-feature conditionals across compiler phases.

When a target range spans multiple supported PySpark lines, Structure should prefer the oldest compatible API that keeps
the output clear and optimizer-visible.

## Alternative Backend Scope

Alternative backend support is specified in [AlternativeBackends.md](AlternativeBackends.back.md)). The compatibility promise
applies to compiler-visible Structure source, not to hook bodies. Hooks are target-specific opaque runtime code and must
either declare `target_backend` or inherit a configured `hook_target_default`.

Future backend work is Python-hosted: v2-v4 prioritize PySpark-family targets such as Spark SQL and typed PySpark
DataFrame patterns. Polars LazyFrame, DuckDB, Ibis, and other non-PySpark backend expansion begin only after v4. Other
targets should come through Ibis when Ibis supports them. Dask DataFrame and Ray Dataset remain out of scope until after
the relational core is stable.
Unsupported active-target requirements must fail before online execution or generation. Multi-target compatibility
checks may report non-active target issues as unsupported, degraded, opaque, or unknown.

## Spark Connect Scope

Spark Connect is a PySpark target variant, not a separate backend id. The configuration shape is:

```toml
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "spark-connect"
```

Mainstream online/generated execution targets ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs. Sprint
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
Unsupported variant capabilities must fail through backend capability diagnostics before online execution or generated
code is claimed compatible. The detailed support contract is specified in [SparkConnect.md](SparkConnect.back.md)).

## Semantic Versioning

After 1.0, Structure follows semantic versioning.

Major releases may:

- change public DSL behavior;
- change online runtime API behavior;
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
- improve online execution without changing semantics;
- add compiler traceability fields in a backward-compatible way.

Patch releases may:

- fix bugs;
- refine diagnostics without changing outcomes;
- fix documentation;
- improve internal implementation without changing public behavior.

Before 1.0, minor releases may change public contracts, but every breaking change should include migration notes.

## Online Runtime Compatibility

Online execution is the default v1 runtime surface. Compatible online execution means:

- transform invocations bind declared input DataFrames by name;
- `StructureSession` accepts caller-owned Spark sessions and optional hook context;
- online execution preserves the same transform semantics as generated PySpark for supported v1 features;
- compiler commands remain Spark-free even though online runtime execution may import PySpark.

Breaking changes to `StructureSession`, transform invocation binding, or online/generated semantic parity require a
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
- a link to [Configuration.md](../Configuration.md) or [Compatibility.md](../Compatibility.md) when the problem is compatibility-related.

New optional keys may appear in minor releases. Removing or changing a documented key requires a major version after
1.0. Deprecated keys should warn before removal when practical.
