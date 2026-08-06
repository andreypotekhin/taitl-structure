# Compatibility

Structure compatibility has four public surfaces:

- the Structure source DSL and configuration users write;
- the direct runtime behavior users execute through `StructureSession`;
- the generated PySpark code optionally committed to user projects;
- optional metadata artifacts such as compiler provenance and static dataflow traceability.

This page defines the public compatibility policy for the initial release and the versioning rules after the
first stable release.

## Initial Baseline

Structure targets:

- Python 3.11 and newer;
- PySpark 3.5.x and 4.0.x, expressed as `plugin.pyspark.profile = ">=3.5,<4.1"` by default;
- ordinary PySpark, expressed as `plugin.pyspark.variant = "ordinary"` by default;
- Linux runtime environments for execution and generated-code execution;
- Linux and macOS development environments;
- Airflow and other schedulers without a hard runtime dependency on them.

Windows development should remain usable where practical. Linux is the runtime target for Spark jobs.

## PySpark Targeting

Set the runtime target in project configuration:

```toml
[tool.structure]
execution_mode = "online"

[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "ordinary"
```

`execution_mode` is `online` by default. Projects may set it to `generated` when runtime execution should go
through checked-in generated classes.

The `plugin.pyspark.profile` value constrains which PySpark APIs execution and generated-code execution may use. Structure should avoid
APIs outside that range unless the user explicitly changes the target.

`plugin.pyspark.variant` selects the PySpark runtime variant. `ordinary` is the default in-process PySpark contract.
`spark-connect` uses Spark Connect through the PySpark DataFrame and Column API.

When a transform uses a feature that cannot run for the configured target, Structure should fail during
`structure check`, `structure compile`, or direct runtime compilation with a backend capability diagnostic.
Unknown plugin targets use `BACKEND-E2401`; unsupported target features use `BACKEND-E2402`.

## API Compatibility

The public API compatibility surface is the compiler-visible Structure DSL plus the admitted PySpark plugin API listed
in [APICatalog.md](APICatalog.md). A catalog row marked `supported` or `implemented` is part of the current public
contract for its stated target profile. Rows marked `planned`, `scheduled`, `partial`, `deferred`, `unsupported`, or
`intentional raw` are not compatibility promises beyond the exact boundary stated in the catalog.

Compatible API additions may appear in minor releases when they preserve existing source behavior and pass backend
capability checks for their target profile. Removing a supported catalog row, changing its public spelling, widening
or narrowing its result schema/nullability in a breaking way, or changing documented semantics requires a major version
after 1.0 or an explicit compatibility shim.

PySpark-plugin additions on top of PySpark are summarized in [APIExtensions.md](APIExtensions.md). Detailed API
reference material remains in [API.ref.md](reference/API.ref.md).

## Spark Connect

Spark Connect is a PySpark target variant, not a separate backend id:

```toml
[tool.structure]

[tool.structure.plugin]
default = "pyspark"

[tool.structure.plugin.pyspark]
profile = ">=3.5,<4.1"
variant = "spark-connect"
```

Ordinary PySpark is the default target. Spark Connect supports completed compiler-visible batch features; streaming
transforms remain caller-owned ordinary PySpark work. The streaming API coverage ledger hardens admitted Structured
Streaming transform shapes while callers retain source, sink, checkpoint, trigger, output-mode, and query lifecycle
ownership.

Spark Connect must not change public DSL syntax, generated class APIs, transform `run(...)` signatures, generated-code
review shape, or streaming orchestration semantics. It must also avoid classic-only internals such as SparkContext,
RDDs, direct JVM/Py4J access, `_jdf`, and private classic PySpark fields.

## Semantic Versioning

After 1.0.0, Structure follows semantic versioning:

- `MAJOR` versions may change public DSL, configuration, runtime helper APIs, generated-code contracts, or
  supported Python/PySpark ranges.
- `MINOR` versions may add compatible DSL features, config keys, diagnostics, generated-code improvements, and
  support for newer Python or PySpark versions.
- `PATCH` versions should contain bug fixes, documentation fixes, and compatible diagnostic improvements.

Before 1.0.0, minor versions may still change public contracts.

Dropping a supported Python or PySpark line is normally a major-version change. A line that is already
unsupported by its upstream project may be dropped in a minor release.

## Internal Versioning

Internal versions follow vN notation (v1 etc.). Decimal positions in N correspond to major, minor and patch
positions in semantic version: v132 is same as the semantic version 1.3.2.

## Execution Compatibility

Execution is the default runtime surface. Compatible execution means:

- transform invocations use declared input names;
- `StructureSession` accepts caller-owned Spark sessions and optional hook context;
- execution preserves the same transform semantics as generated-code execution for supported initial-release
  features;
- compiler commands remain Spark-free even though execution may import PySpark.

Breaking changes to `StructureSession`, transform invocation binding, or execution/generated-code semantic parity
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

- ordinary reachable helpers and classes for reusable compiler-visible expression logic, with optional `@special(type="expr")` metadata;
- explicit, source-ordered `@raw` hooks for arbitrary PySpark DataFrame code.

These paths have different guarantees. Ordinary reachable helper logic and `@special(type="expr")` logic participate in
compileability checks, generated code, traceability, and backend capability diagnostics. Code marked
`@special(type="ignore")` is rejected when reached from compiled logic. Hook bodies are opaque: Structure validates the hook
declaration, calls the hook at the documented lifecycle point, and records the boundary, but it does not
inspect arbitrary PySpark code inside the hook.

Backend capability providers, diagnostic renderers, schema type adapters, validation policy plugins, and hook
lint rule registries are internal or deferred extension surfaces. Projects should not depend on
monkey-patching those internals. Future releases may promote some of them to public APIs once their behavior,
compatibility, and tests are specified.

## Plugin Compatibility

The public plugin contract is Plugin API v1. It lets Structure Core invoke one selected plugin through a negotiated
`PluginAPI` facade without importing the plugin's DSL, runtime, schema materialization, or lowered plan types. The
complete authoring contract is [PluginAuthoring.md](dev/PluginAuthoring.md), and the detailed v1 specification is
[PluginAPI.md](dev/specifications/PluginAPI.md).

The bundled PySpark plugin is selected by:

```toml
[tool.structure.plugin]
default = "pyspark"
```

External plugins are discovered from the `structure.plugin` entry-point group. A plugin exposes a descriptor with its
plugin name, distribution identity, plugin version, and inclusive minimum/maximum Plugin API versions. Core selects the
highest v1-compatible version in the overlap and asks the plugin for exactly one `PluginAPI` facade for that version.
No overlap, an unadvertised version, descriptor identity mismatch, or a missing required facet is a plugin
compatibility error.

Plugin API v1 has four required facets and three optional lifecycle facets:

| Facet | Compatibility role |
| --- | --- |
| `schema` | Validates plugin-owned fields and materializes target schema representations before compilation. |
| `authoring` | Supplies symbolic step arguments and captures returned plugin bodies while Core owns transform lifecycle and source order. |
| `compiler` | Lowers Core-owned transform facts plus opaque authored bodies into a plugin-owned compiled payload. |
| `capabilities` | Resolves target feature support from plugin options without requiring a live runtime. |
| `executor` | Optional; runs an opaque compiled payload over caller-supplied runtime objects. |
| `generator` | Optional; returns generated file content while Core owns paths and writes. |
| `serializer` | Optional; encodes and decodes only opaque plugin payloads inside Core-owned artifact envelopes. |

A plugin may add compiler-visible API only when its public rows are documented in [APICatalog.md](APICatalog.md), backed
by capability diagnostics, and compatible with the selected profile. Unknown plugins fail with `BACKEND-E2401`; known
plugins that cannot support a requested feature for the configured target fail with `BACKEND-E2402`.

Generated artifacts and serialized payloads record plugin name, normalized distribution identity, plugin version,
negotiated Plugin API version, selected options, payload version, and fingerprints. Loading an artifact must use the
recorded compatible plugin/API combination or rebuild the artifact; Structure must not silently reinterpret an opaque
payload through a different plugin contract.

The internal provider interfaces behind plugin capabilities, rendering, diagnostics, type adapters, and validation
policies are not public extension APIs yet. Do not depend on monkey-patching or subclassing those internals unless a
future catalog row and specification explicitly promote them.

## Compiler Traceability Schema Versioning

Compiler traceability has two metadata models:

- compiler provenance, which maps source nodes to IR nodes to generated PySpark nodes;
- static dataflow traceability, which records transform, table, and column dependencies inferred from IR.

Traceability schema rules:

- Breaking metadata-shape changes bump the traceability schema major version.
- Additive fields bump the traceability schema minor version.
- Consumers should ignore unknown fields.
- Structure should keep default compiler traceability compact and stable across patch releases.

Runtime LDJSON traceability is not part of the current compatibility contract. It is tracked as a
[nice-to-have](dev/project-management/NiceToHave.md).

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

## Current Boundary

Structure supports execution and generated PySpark transformations, completed Spark Connect batch features, and
compiler-visible streaming transformations backed by the checked streaming API coverage ledger. Loading, storage,
streaming lifecycle, orchestration, alternative backends, and non-batch Spark Connect work remain outside the current
contract unless a later decision explicitly admits them.
