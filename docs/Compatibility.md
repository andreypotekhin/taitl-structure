# Compatibility

Structure has three compatibility surfaces:

- the Structure source DSL and configuration users write;
- the online runtime behavior users execute through `StructureSession`;
- the generated PySpark code optionally committed to user projects;
- optional metadata artifacts such as compiler provenance and static dataflow lineage.

This page defines the public compatibility policy for the initial release and the planned versioning rules after the
first stable release.

## Initial Baseline

Structure targets:

- Python 3.11 and newer;
- PySpark 3.5.x and 4.0.x, expressed as `target_pyspark = ">=3.5,<4.1"` by default;
- Linux runtime environments for online and generated PySpark execution;
- Linux and macOS development environments;
- Airflow and other schedulers without a hard runtime dependency on them.

Windows development should remain usable where practical, but Linux is the runtime target for Spark jobs.

## PySpark Targeting

Set the runtime target in project configuration:

```toml
[tool.structure]
execution_mode = "online"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
```

The `execution_mode` value is `online` by default. Projects may set it to `generated` when they want runtime execution
through checked-in generated classes.

The `target_pyspark` value constrains which PySpark APIs online and generated execution may use. Structure should avoid
APIs outside that range unless the user explicitly changes the target.

When a transform uses a feature that cannot run for the configured target, Structure should fail during `structure
check`, `structure compile`, or online runtime compilation with a backend capability diagnostic. Unknown backend targets
use `BACKEND-E2401`; unsupported backend features use `BACKEND-E2402`.

## Spark Connect

Spark Connect is not part of the initial release, v2, or v3 commitment. The initial release and v2 online/generated
execution target ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs. v3 adds streaming orchestration on
top of the ordinary PySpark contract.

Spark Connect support is scheduled for v4 as backend expansion work. It may land earlier only if it can be implemented
through the existing PySpark target boundary without changing public APIs, generated-code shape, streaming orchestration
semantics, or compatibility guarantees.

## Semantic Versioning

After Structure reaches 1.0, public releases follow semantic versioning:

- `MAJOR` versions may change public DSL, configuration, runtime helper APIs, generated-code contracts, or supported
  Python/PySpark ranges.
- `MINOR` versions may add compatible DSL features, config keys, diagnostics, generated-code improvements, and support
  for newer Python or PySpark versions.
- `PATCH` versions should contain bug fixes, documentation fixes, and compatible diagnostic improvements.

Before 1.0, minor versions may still change public contracts, but each release should document migration steps.

Dropping a supported Python or PySpark line is normally a major-version change. A line that is already unsupported by
its upstream project may be dropped in a minor release if the release notes include a clear migration note.

## Online Runtime Compatibility

Online execution is the default runtime surface. Compatible online execution means:

- transform invocations use declared input names;
- `StructureSession` accepts caller-owned Spark sessions and optional hook context;
- online execution preserves the same transform semantics as generated PySpark for supported initial-release features;
- compiler commands remain Spark-free even though online runtime execution may import PySpark.

Breaking changes to `StructureSession`, transform invocation binding, or online/generated semantic parity require a
major version after 1.0 or a compatibility shim.

## Generated-Code Compatibility

Generated PySpark is optional committed build output owned by the Structure compiler. Regenerate it after upgrading
Structure when your project commits generated files or uses `execution_mode = "generated"`.

Compatibility rules:

- Generated code should declare the Structure generator version and target PySpark range in a header comment.
- Generated code may depend on Structure runtime helpers only through documented generated-runtime APIs.
- Runtime helper breaking changes require either a major Structure version or a compatibility shim.
- CI should run `structure compile --fail-on-diff` after upgrades for projects that commit generated files.

Generated code is readable and reviewable, but not hand-edited. Change source Structure code, configuration, or the
compiler instead.

## Extension Compatibility

Structure keeps the initial extension surface narrow. Supported public extension points are:

- `@expr_fn` helpers for reusable compiler-visible expression logic;
- explicit `@before(...)` and `@after(...)` hooks for arbitrary PySpark DataFrame code at named step boundaries.

These two paths have different guarantees. `@expr_fn` logic participates in compileability checks, generated code,
lineage, and backend capability diagnostics. Hook bodies are opaque: Structure validates the hook declaration, calls the
hook at the documented lifecycle point, and records the hook boundary, but it does not inspect arbitrary PySpark code
inside the hook.

Backend capability providers, diagnostic renderers, schema type adapters, validation policy plugins, and hook lint rule
registries are internal or deferred extension surfaces. Projects should not depend on monkey-patching those internals.
Future releases may promote some of them to public APIs once their behavior, compatibility, and tests are specified.

## Compiler Lineage Schema Versioning

Compiler lineage covers two metadata models:

- compiler provenance, which maps source nodes to IR nodes to generated PySpark nodes;
- static dataflow lineage, which records transform, table, and column dependencies inferred from IR.

Lineage schema rules:

- Breaking metadata-shape changes bump the lineage schema major version.
- Additive fields bump the lineage schema minor version.
- Consumers should ignore unknown fields.
- Structure should keep default compiler lineage compact and stable across patch releases.

Runtime LDJSON lineage is not part of the initial compatibility contract. It is tracked as a nice-to-have beyond v4 in
`docs/dev/project-management/NiceToHave.md`.

## Config Schema Versioning

Configuration has an implicit schema version for the initial release. A future explicit key may make this visible:

```toml
config_schema_version = 1
```

Config schema rules:

- Unknown keys and invalid values are errors with structured diagnostics.
- New optional keys may be added in minor versions.
- Removing or changing the meaning of a documented key requires a major version after 1.0.
- Deprecated keys should produce warnings before removal when practical.

## Roadmap

v2 expands online/generated PySpark features and adoption tooling while preserving the same basic compatibility
contract.

v3 adds streaming orchestration once transform compilation is stable.

v4 adds Spark Connect support when it can be specified, tested, and documented without weakening online execution or the
generated-code review model.
