# Compatibility Policy Specification

This specification resolves `Challenges.md` C19, "Versioning and Compatibility Policy Are Missing".

The policy is public-facing in `docs/Compatibility.md`. This specification defines the development contract behind that
page.

## Goals

The compatibility policy must:

- define supported Python versions;
- define supported PySpark versions and the default `target_pyspark` range;
- define Spark Connect scope;
- define semantic versioning expectations;
- define generated-code compatibility;
- define compiler lineage schema versioning;
- define config schema versioning.

## v1 Runtime Baseline

Structure v1 supports Python 3.11 and newer.

The default PySpark target is:

```toml
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
```

This means generated code should target PySpark 3.5.x and 4.0.x APIs unless the user configures a different target.

Airflow is not a hard dependency. Generated code should be importable from Airflow, Spark jobs, notebooks, or other
orchestrators without pulling in scheduler-specific runtime dependencies.

Linux is the v1 runtime target. Linux and macOS are the v1 development targets. Windows development may work where the
toolchain allows it, but generated Spark jobs should be designed and tested primarily for Linux deployment.

## PySpark Version Targeting

The PySpark emitter owns PySpark API compatibility. Discovery, symbolic execution, IR checks, lineage, and generic
diagnostics must not scatter PySpark-version conditionals unless a narrow check directly belongs there.

The emitter must be version-aware enough to:

- avoid APIs outside the configured `target_pyspark` range;
- reject requested DSL features that cannot be generated for that range;
- produce diagnostics that state the required PySpark version when a feature is unavailable;
- keep generated output deterministic for the same source, config, and Structure version.

When a target range spans multiple supported PySpark lines, generated code should prefer the oldest compatible API that
keeps the output clear and optimizer-visible.

## Spark Connect Scope

Spark Connect is deferred to v4.

v1 and v2 generated code targets ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs. v3 adds streaming
orchestration on top of that ordinary PySpark contract. The compiler must not claim Spark Connect support in public
docs or diagnostics before a tested contract exists.

Spark Connect may be implemented before v4 only if all of these are true:

- it uses the existing PySpark emitter boundary cleanly;
- it does not change public DSL syntax;
- it does not change generated class construction or `run(...)` signatures;
- it does not change streaming orchestration semantics;
- it does not weaken generated-code readability or reviewability;
- it has compatibility tests for the supported PySpark Connect versions;
- public docs make the support level explicit.

Otherwise, Spark Connect belongs in v4 as backend expansion work.

## Semantic Versioning

After 1.0, Structure follows semantic versioning.

Major releases may:

- change public DSL behavior;
- remove or change documented config keys;
- change generated-runtime helper contracts;
- change generated-code compatibility rules;
- drop supported Python or PySpark lines;
- make breaking compiler lineage or config schema changes.

Minor releases may:

- add DSL features;
- add config keys with defaults;
- add PySpark support;
- add diagnostics;
- improve generated code without changing semantics;
- add compiler lineage fields in a backward-compatible way.

Patch releases may:

- fix bugs;
- refine diagnostics without changing outcomes;
- fix documentation;
- improve internal implementation without changing public behavior.

Before 1.0, minor releases may change public contracts, but every breaking change should include migration notes.

## Generated-Code Compatibility

Generated PySpark is committed build output owned by the Structure compiler.

The generator should include a compact header in generated files with:

- Structure generator version;
- configured backend;
- configured PySpark target range;
- source module or transform identity where useful.

Generated code may import Structure generated-runtime helpers. Those helpers are public to generated code, even if they
are not intended for direct end-user use.

Breaking generated-runtime helper changes require one of:

- a major Structure version;
- a compatibility shim;
- a regeneration strategy that makes old generated code fail with a clear upgrade diagnostic.

Upgrade guidance must tell users to run:

```bash
structure compile --fail-on-diff
```

## Compiler Lineage Schema Versioning

Compiler lineage has two v1 metadata models:

- compiler provenance, which maps source nodes to IR nodes to generated PySpark nodes;
- static dataflow lineage, which records transform, table, and column dependencies inferred from IR.

The lineage schema version follows `major.minor`.

Breaking changes require a major lineage schema version bump. Additive fields require a minor version bump. Consumers
should ignore unknown fields so minor additions remain compatible.

Runtime LDJSON lineage is not part of the v1 compatibility contract. It is tracked as a nice-to-have beyond v4 in
`docs/dev/project-management/NiceToHave.md`.

## Config Schema Versioning

Config schema versioning is implicit for v1. A future explicit key may expose it:

```toml
config_schema_version = 1
```

Unknown config keys and invalid values are errors. The diagnostic must include:

- the setting path;
- the invalid value;
- allowed values or expected type when known;
- a link to `docs/Configuration.md` or `docs/Compatibility.md` when the problem is compatibility-related.

New optional keys may appear in minor releases. Removing or changing a documented key requires a major version after
1.0. Deprecated keys should warn before removal when practical.

## Acceptance Criteria

- `docs/Compatibility.md` documents the public policy.
- `Readme.md` links to the compatibility policy.
- `docs/Configuration.md` documents `target_backend`, `target_pyspark`, and compatibility diagnostics.
- `docs/dev/Roadmap.md` and public roadmap text schedule Spark Connect for v4.
- `docs/dev/design/Challenges.md` marks C19 as resolved by this specification and the public policy.
- The seed config default is `target_pyspark = ">=3.5,<4.1"`.
