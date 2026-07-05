# Compatibility

Structure compatibility has four public surfaces:

- the Structure source DSL and configuration users write;
- the online runtime behavior users execute through `StructureSession`;
- the generated PySpark code optionally committed to user projects;
- optional metadata artifacts such as compiler provenance and static dataflow traceability.

This page defines the public compatibility policy for the initial release and the versioning rules after the
first stable release.

## Initial Baseline

Structure targets:

- Python 3.11 and newer;
- PySpark 3.5.x and 4.0.x, expressed as `target_profile = ">=3.5,<4.1"` by default;
- ordinary PySpark, expressed as `target_variant = "ordinary"` by default;
- Linux runtime environments for online and generated PySpark execution;
- Linux and macOS development environments;
- Airflow and other schedulers without a hard runtime dependency on them.

Windows development should remain usable where practical. Linux is the runtime target for Spark jobs.

## PySpark Targeting

Set the runtime target in project configuration:

```toml
[tool.structure]
execution_mode = "online"
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "ordinary"
```

`execution_mode` is `online` by default. Projects may set it to `generated` when runtime execution should go
through checked-in generated classes.

The `target_profile` value constrains which PySpark APIs online and generated execution may use. Structure should avoid
APIs outside that range unless the user explicitly changes the target.

`target_variant` selects the PySpark runtime variant. `ordinary` is the default in-process PySpark contract.
`spark-connect` uses Spark Connect through the PySpark DataFrame and Column API.

When a transform uses a feature that cannot run for the configured target, Structure should fail during
`structure check`, `structure compile`, or online runtime compilation with a backend capability diagnostic.
Unknown backend targets use `BACKEND-E2401`; unsupported backend features use `BACKEND-E2402`.

## Spark Connect

Spark Connect is a PySpark target variant, not a separate backend id:

```toml
[tool.structure]
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
target_variant = "spark-connect"
```

The initial release and mainstream v2 online/generated execution target ordinary PySpark `SparkSession`, `DataFrame`,
and `Column` APIs. Spark Connect is experimental for completed v1/v2 batch features and is covered by the integration
matrix lanes `spark-connect35` and `spark-connect40`. V3 adds streaming orchestration on top of the ordinary PySpark
contract.

Spark Connect must not change public DSL syntax, generated class APIs, transform `run(...)` signatures, generated-code
review shape, or streaming orchestration semantics. It must also avoid classic-only internals such as SparkContext,
RDDs, direct JVM/Py4J access, `_jdf`, and private classic PySpark fields. Full support is a later promotion decision
after parity evidence, diagnostics, and CI coverage exist.

## Semantic Versioning

After 1.0, Structure follows semantic versioning:

- `MAJOR` versions may change public DSL, configuration, runtime helper APIs, generated-code contracts, or
  supported Python/PySpark ranges.
- `MINOR` versions may add compatible DSL features, config keys, diagnostics, generated-code improvements, and
  support for newer Python or PySpark versions.
- `PATCH` versions should contain bug fixes, documentation fixes, and compatible diagnostic improvements.

Before 1.0, minor versions may still change public contracts, but each release should document migration
steps.

Dropping a supported Python or PySpark line is normally a major-version change. A line that is already
unsupported by its upstream project may be dropped in a minor release if the release notes include a clear
migration note.

## Online Runtime Compatibility

Online execution is the default runtime surface. Compatible online execution means:

- transform invocations use declared input names;
- `StructureSession` accepts caller-owned Spark sessions and optional hook context;
- online execution preserves the same transform semantics as generated PySpark for supported initial-release
  features;
- compiler commands remain Spark-free even though online runtime execution may import PySpark.

Breaking changes to `StructureSession`, transform invocation binding, or online/generated semantic parity
require a major version after 1.0 or a compatibility shim.

## Generated-Code Compatibility

Generated PySpark is optional committed build output owned by the Structure compiler. Regenerate it after
upgrading Structure when your project commits generated files or uses `execution_mode = "generated"`.

Compatibility rules:

- Generated code should declare the Structure generator version and target PySpark range in a header comment.
- Generated code may depend on Structure runtime helpers only through documented generated-runtime APIs.
- Runtime helper breaking changes require either a major Structure version or a compatibility shim.
- CI should run `structure compile --fail-on-diff` after upgrades for projects that commit generated files.

Generated code is readable and reviewable, but not hand-edited. Change Structure source, configuration, or the
compiler instead.

## Extension Compatibility

Structure keeps the initial extension surface narrow:

- `@expr_fn` helpers for reusable compiler-visible expression logic;
- explicit `@before(...)` and `@after(...)` hooks for arbitrary PySpark DataFrame code at named step
  boundaries.

These paths have different guarantees. `@expr_fn` logic participates in compileability checks, generated code,
traceability, and backend capability diagnostics. Hook bodies are opaque: Structure validates the hook
declaration, calls the hook at the documented lifecycle point, and records the boundary, but it does not
inspect arbitrary PySpark code inside the hook.

Backend capability providers, diagnostic renderers, schema type adapters, validation policy plugins, and hook
lint rule registries are internal or deferred extension surfaces. Projects should not depend on
monkey-patching those internals. Future releases may promote some of them to public APIs once their behavior,
compatibility, and tests are specified.

## Compiler Traceability Schema Versioning

Compiler traceability has two metadata models:

- compiler provenance, which maps source nodes to IR nodes to generated PySpark nodes;
- static dataflow traceability, which records transform, table, and column dependencies inferred from IR.

Traceability schema rules:

- Breaking metadata-shape changes bump the traceability schema major version.
- Additive fields bump the traceability schema minor version.
- Consumers should ignore unknown fields.
- Structure should keep default compiler traceability compact and stable across patch releases.

Runtime LDJSON traceability is not part of the initial compatibility contract. It is tracked as a nice-to-have
beyond v4 in [NiceToHave.md](dev/project-management/NiceToHave.md).

## Config Schema Versioning

Configuration has an implicit schema version for the initial release. A future explicit key may make this
visible:

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
contract. Experimental Spark Connect parity covers completed v1/v2 batch features through the PySpark target variant
`spark-connect`.

v3 adds streaming orchestration once transform compilation is stable.

v4 promotes Spark Connect from experimental to supported if parity evidence, diagnostics, and CI are complete;
otherwise v4 continues hardening the variant.
